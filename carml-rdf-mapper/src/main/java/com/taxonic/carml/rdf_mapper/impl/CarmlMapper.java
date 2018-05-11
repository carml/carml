package com.taxonic.carml.rdf_mapper.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.PropertyHandler;
import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfResourceName;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.rdf_mapper.qualifiers.PropertyPredicate;
import com.taxonic.carml.rdf_mapper.qualifiers.PropertySetter;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Qualifier;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class CarmlMapper implements Mapper, MappingCache {

	// type is the exact type of instance we need; eg. NOT a supertype.
	// so no unbound type parameters. no interface, unless a list or so.
	// TODO *could* be an interface, but then an implementation type should be registered.
	@Override
	public <T> T map(Model model, Resource resource, Set<Type> types) {


		if (types.size() > 1) {
			if (!types.stream().allMatch(t -> ((Class<?>)t).isInterface())) {
				throw new RuntimeException("In case of multiple types, mapper requires all types to be interfaces");
			}
		}

		if (types.stream().allMatch(t -> ((Class<?>)t).isInterface())) {
			return doMultipleInterfaceMapping(model, resource, types);
		} else {
			return doSingleTypeConcreteClassMapping(model, resource, Iterables.getOnlyElement(types));
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T doMultipleInterfaceMapping(Model model, Resource resource, Set<Type> types) {
		List<Type> implementations = types.stream().map(this::getInterfaceImplementation).collect(Collectors.toList());

		Map<Type, Object> implementationsToDelegates = implementations.stream()
				.collect(Collectors.toMap(t -> t, t-> doSingleTypeConcreteClassMapping(model, resource, t)));

		Map<Type, List<Method>> implementationMethods =
				implementations.stream().collect(Collectors.toMap(
						t -> t,
						t -> gatherMethods((Class<?>) t).collect(Collectors.toList())));

		BiFunction<Type, Method, Boolean> implementationHasMethod =
				(t, m) -> implementationMethods.get(t).contains(m);


		return (T) Proxy.newProxyInstance(CarmlMapper.class.getClassLoader(), types.stream().toArray(Class<?>[]::new),
				(proxy, method, args) -> {

					Optional<Object> delegate = implementations.stream()
							.filter(t -> implementationHasMethod.apply(t, method))
							.map(implementationsToDelegates::get) // TODO can this ever be null? this case is not checked below
							.findFirst();

					if (!delegate.isPresent()) {
						throw new RuntimeException(String.format("no implementation present with specified method [%s]", method));
					}

					return
					delegate.map(d -> {
 						try {
							return method.invoke(d, args);
						} catch (IllegalAccessException | IllegalArgumentException
								| InvocationTargetException e) {
							throw new RuntimeException(
									String.format(
											"error trying to invoke method [%s] on delegate [%s]",
												method, d), e);
						}
					})
					// since we know 'delegate' is present, the case
					// here means the invoked method returned null. so we
					// return null.
					.orElse(null);
				});
	}

	private Stream<Method> gatherMethods(Class<?> clazz) {
		return Stream.concat(
				Arrays.asList(clazz.getDeclaredMethods()).stream(),
				Stream.concat(
						Arrays.asList(clazz.getInterfaces()).stream().flatMap(this::gatherMethods),
						Optional.ofNullable(clazz.getSuperclass())
							.map(this::gatherMethods)
							.orElse(Stream.empty())
				)
//				.filter(m -> Modifier.isPublic(m.getModifiers()))
		);
	}

	private <T> T doSingleTypeConcreteClassMapping(Model model, Resource resource, Type type) {

		Class<?> c = (Class<?>) type;

		if (c.isEnum()) {
			throw new RuntimeException("cannot create an instance of "
				+ "enum type [" + c.getCanonicalName() + "]. you should probably "
					+ "place an instance of the enum type in the MappingCache "
					+ "prior to mapping.");
		}

		// build meta-model
		// TODO cache
		List<PropertyHandler> propertyHandlers =
				Stream.concat(
						Arrays.asList(c.getMethods()).stream()
						.map(m -> getRdfPropertyHandler(m, c))
						.filter(Objects::nonNull)
				,
						Arrays.asList(c.getMethods()).stream()
						.map(m -> getRdfResourceNameHandler(m, c))
						.filter(Objects::nonNull)
				)
				.collect(Collectors.toList());

		Object instance;
		try {
			instance = c.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("failed to instantiate [" + c.getCanonicalName() + "]", e);
		}

		propertyHandlers.forEach(h ->
			h.handle(model, resource, instance)
		);


		// TODO error if mandatory properties are not present in triples?

		// TODO error if excess/unmapped triples?

		// TODO queue instead of recursion

		@SuppressWarnings("unchecked")
		T result = (T) instance;
		return result;
	}

	private static class PropertyType {

		private Class<?> elementType;
		private Class<?> iterableType;

		PropertyType(Class<?> elementType, Class<?> iterableType) {
			this.elementType = elementType;
			this.iterableType = iterableType;
		}

		Class<?> getElementType() {
			return elementType;
		}

		Class<?> getIterableType() {
			return iterableType;
		}
	}

	private PropertyType determinePropertyType(Method setter) {

		Type type = setter.getGenericParameterTypes()[0];
		Class<?> elementType = null; // XXX assuming Class<?> for now
		Class<?> iterableType = null;

		// if property type is X<E>, where X <= Iterable, use E as property type from now on
		// XXX not sure if proper place
		if (type instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) type;
			Class<?> rawType = (Class<?>) pt.getRawType();
			if (Iterable.class.isAssignableFrom(rawType)) {
				elementType = (Class<?>) pt.getActualTypeArguments()[0];
				iterableType = rawType;
			}
		}

		// otherwise, use property type as-is
		if (elementType == null) {
			elementType = (Class<?>) type;
		}

		return new PropertyType(elementType, iterableType);
	}

	private Class<?> getTypeFromRdfTypeAnnotation(Method method) {
		RdfType annotation = method.getAnnotation(RdfType.class);
		if (annotation == null) {
			return null;
		}
		return annotation.value();
	}

	private TypeDecider getTypeDeciderFromAnnotation(Method method) {
		com.taxonic.carml.rdf_mapper.annotations.RdfTypeDecider annotation = method
			.getAnnotation(com.taxonic.carml.rdf_mapper.annotations.RdfTypeDecider.class);
		if (annotation != null) {
			Class<?> deciderClass = annotation.value();
			try {
				return (TypeDecider) deciderClass.newInstance();
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException("failed to instantiate rdf type decider class " + deciderClass.getCanonicalName(), e);
			}
		}
		return null;
	}

	private TypeDecider createTypeDecider(Method method, Class<?> elementType) {

		// if @RdfTypeDecider(Xyz.class) is present on property, use that
		TypeDecider typeDecider = getTypeDeciderFromAnnotation(method);
		if (typeDecider != null) {
			return typeDecider;
		}

		// if @RdfType(MyImpl.class) is present on property, use that
		Class<?> typeFromRdfTypeAnnotation = getTypeFromRdfTypeAnnotation(method);
		if (typeFromRdfTypeAnnotation != null) {
			return (m, r) -> ImmutableSet.of(typeFromRdfTypeAnnotation);
		}

		// backup: use this property's type.
		// if that's an interface, use registered implementation thereof, if any.
		// (mapper.registerImplementation(Xyz.class, XyzImpl.class)
		Class<?> implementationType = elementType; // TODO mapper.getRegisteredTypeAlias/Implementation(propertyType);
		// TODO check for pre: no unbound parameter types; not an interface
		TypeDecider propertyTypeDecider = (m, r) -> ImmutableSet.of(implementationType);

		// use rdf:type triple, if present.
		//   => the rdf:type value would have to correspond to an @RdfType annotation on a registered class.
		//      TODO probably scan propertyType for @RdfType to register it, here or elsewhere..
		return new TypeFromTripleTypeDecider(this, Optional.of(propertyTypeDecider));

	}

	private Supplier<Collection<Object>> createCollectionFactory(Class<?> iterableType) {
		if (iterableType == null) {
			return null;
		}
		// TODO Map<Class<?>, Supplier<Collection<Object>>>
		if (iterableType.equals(Set.class)) {
			return LinkedHashSet::new;
		} else if (iterableType.equals(List.class)) {
			return LinkedList::new;
		}
		throw new RuntimeException("don't know how to create a factory for collection type [" + iterableType.getCanonicalName() + "]");
	}

	private Function<Collection<Object>, Collection<Object>> createImmutableTransform(Class<?> iterableType) {
		if (iterableType == null) {
			return null;
		}
		if (iterableType.equals(Set.class)) {
			return x -> Collections.unmodifiableSet((Set<Object>) x);
		} else if (iterableType.equals(List.class)) {
			return x -> Collections.unmodifiableList((List<Object>) x);
		}
		throw new RuntimeException("don't know how to create a transform to make collections of type [" + iterableType.getCanonicalName() + "] immutable");
	}

	private static interface DependencyResolver {

		Object resolve(Type type, List<Annotation> qualifiers);

	}

	private Optional<PropertyHandler> getSpecifiedPropertyHandler(
		RdfProperty annotation,
		DependencyResolver resolver
	) {

		if (annotation.handler().equals(PropertyHandler.class)) {
			return Optional.empty();
		}

		Class<? extends PropertyHandler> handlerCls = annotation.handler();
		PropertyHandler handler;
		try {
			handler = handlerCls.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("could not instantiate specified "
				+ "PropertyHandler class [" + handlerCls.getCanonicalName() + "]");
		}

		// do dependency injection through setter methods annotated with @Inject
		List<Consumer<Object>> setterInjectors =
			Arrays.asList(handlerCls.getMethods()).stream()

				// find methods annotated with @Inject
				.filter(m -> m.getAnnotation(Inject.class) != null)
				.<Consumer<Object>>map(m -> createInvocableSetter(m, resolver))
			.collect(Collectors.toList());

		setterInjectors.forEach(i -> i.accept(handler));

		return Optional.of(handler);
	}

	private Consumer<Object> createInvocableSetter(Method method, DependencyResolver resolver) {
		// gather qualifiers on setter
		List<Annotation> qualifiers =
			Arrays.asList(method.getAnnotations()).stream()
				.filter(a -> a.annotationType().getAnnotation(Qualifier.class) != null)
				.collect(Collectors.toList());

		// determine property/setter type
		List<Type> parameterTypes = Arrays.asList(method.getGenericParameterTypes());
		if (parameterTypes.isEmpty() || parameterTypes.size() > 1) {
			throw new RuntimeException("method [" + method.getName() + "], annotated "
				+ "with @Inject does NOT take exactly 1 parameter; it takes " + parameterTypes.size());
		}
		Type propertyType = parameterTypes.get(0);

		return instance -> setterInvocation(method, resolver, instance, propertyType, qualifiers);
	}

	private Object setterInvocation(
		Method method,
		DependencyResolver resolver,
		Object instance,
		Type propertyType,
		List<Annotation> qualifiers
	) {
		Object propertyValue = resolver.resolve(propertyType, qualifiers);
		try {
			return method.invoke(instance, propertyValue);
		}
		catch (
			IllegalAccessException |
			IllegalArgumentException |
			InvocationTargetException e
		) {
			throw new RuntimeException("error invoking setter [" + method.getName() + "]", e);
		}
	}

	private PropertyHandler getRdfResourceNameHandler(Method method, Class<?> c) {
		RdfResourceName annotation = method.getAnnotation(RdfResourceName.class);
		if (annotation == null) {
			return null;
		}

		String name = method.getName();
		String property = PropertyUtils.getPropertyName(name);

		String setterName = PropertyUtils.createSetterName(property);
		Method setter = findSetter(c, setterName);

		BiConsumer<Object, Object> set = getSetterInvocation(method, c, setter, setterName);

		return new PropertyHandler() {

			@Override
			public void handle(Model model, Resource resource, Object instance) {
				set.accept(instance, resource.stringValue());
			}

		};

	}

	private BiConsumer<Object, Object> getSetterInvocation(Method method, Class<?> c, Method setter, String setterName) {

		// TODO if no setter, set the field directly? (configurable option) ugh
		if (setter == null) {
			throw new RuntimeException("could not find setter [" + setterName + "] with 1 parameter");
		}
		return (i, v) -> {
			try {
//				System.out.println("invoking [" + c.getSimpleName() + "." + setter.getName() + "]");
				setter.invoke(i, v);
			}
			catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RuntimeException("could not invoke setter [" + c.getSimpleName() + "." + setter.getName() + "]", e);
			}
		};
	}

	private PropertyHandler getRdfPropertyHandler(Method method, Class<?> c) {

		RdfProperty annotation = method.getAnnotation(RdfProperty.class);
		if (annotation == null) {
			return null;
		}

		String name = method.getName();
		String property = PropertyUtils.getPropertyName(name);

		String setterName = PropertyUtils.createSetterName(property);
		Method setter = findSetter(c, setterName);

		BiConsumer<Object, Object> set = getSetterInvocation(method, c, setter, setterName);

		PropertyType propertyType = determinePropertyType(setter);
		Class<?> elementType = propertyType.getElementType();

		/* TODO valueTransformer is capable of transforming an rdf value
		 * to a type expected by this property.
		 */
		ValueTransformer valueTransformer;

		// if property type is a sub-type of Value, no transform is needed
		if (Value.class.isAssignableFrom(elementType)) {
			valueTransformer = (m, v) -> v;
		} else if (String.class.isAssignableFrom(elementType)) {
			valueTransformer = (m, v) -> ((Literal) v).getLabel();
		} else {

			TypeDecider typeDecider = createTypeDecider(method, elementType);

			Function<Object, Object> typeAdapter = o -> o;
			// TODO use type adapter from @RdfTypeAdapter annotation, if any

			valueTransformer = new ComplexValueTransformer(typeDecider, this, this, typeAdapter);
		}

		ValueFactory f = SimpleValueFactory.getInstance();
		IRI predicate = f.createIRI(annotation.value());

		Class<?> iterableType = propertyType.getIterableType();
		Supplier<Collection<Object>> createIterable = createCollectionFactory(iterableType);
		Function<Collection<Object>, Collection<Object>> immutableTransform = createImmutableTransform(iterableType);

		// get handler from @RdfProperty.handler, if any
		Optional<PropertyHandler> handler =
			getSpecifiedPropertyHandler(
				annotation,
				new DependencyResolver() {

					private Optional<Object> getQualifierValue(Class<? extends Annotation> qualifier) {
						if (qualifier.equals(PropertyPredicate.class)) {
							return Optional.of(predicate);
						}
						if (qualifier.equals(PropertySetter.class)) {
							return Optional.of(set);
						}

						// TODO ..

						return Optional.empty();
					}

					private Optional<Object> getValueByType(Type type) {
						if (type.equals(Mapper.class)) {
							return Optional.of(CarmlMapper.this);
						} else if (type.equals(MappingCache.class)) {
							return Optional.of(CarmlMapper.this);
						}

						// TODO ..

						return Optional.empty();
					}

					@Override
					public Object resolve(Type type, List<Annotation> qualifiers) {

						// try simple mapping of a present qualifier to a value
						Optional<Object> qualifierValue =
							qualifiers.stream()
								.map(Annotation::annotationType)
								.map(t -> getQualifierValue(t))
								.filter(Optional::isPresent)
								.map(Optional::get)
								.findFirst();
						if (qualifierValue.isPresent()) {
							return qualifierValue.get();
						}

						Optional<Object> valueByType = getValueByType(type);
						if (valueByType.isPresent()) {
							return valueByType.get();
						}


						// TODO ..


						throw new RuntimeException(
							"could not resolve dependency for type [" + type + "] and "
								+ "qualifiers [" + qualifiers + "]");
					}
				}
			);
		if (handler.isPresent()) {
			return handler.get();
		}

		return new PropertyHandler() {

			@Override
			public void handle(Model model, Resource resource, Object instance) {

				List<Value> values =
					model.filter(resource, predicate, null)
						.objects()
						.stream()
						.collect(Collectors.toList());

				// no values
				if (values.isEmpty()) {
					// TODO error if property has @RdfRequired?
				}

				// iterable property - set collection value
				if (iterableType != null) {

					List<Object> transformed = values.stream()
						.map(v -> valueTransformer.transform(model, v))
						.collect(Collectors.toList());

					Collection<Object> result = createIterable.get();
					transformed.forEach(result::add);
					Collection<Object> immutable = immutableTransform.apply(result);
					set.accept(instance, immutable);

				}

				// single value property
				else {

					// multiple values present - error
					if (values.size() > 1) {
						throw new RuntimeException("multiple values for property [" + predicate + "], but "
							+ "corresponding java property is NOT an Iterable property");
					}

					if (!values.isEmpty()) {
						Object result = valueTransformer.transform(model, values.get(0));
						set.accept(instance, result);
					}
				}

				// TODO what about languages? @RdfLanguage("nl") to select only 1 language (or multiple?)
				// then, if multiple such values exist in the graph, the property should be a List<String>.

			}
		};
	}

	private static Method findSetter(Class<?> c, String setterName) {
		List<Method> setters =
			Arrays.asList(c.getMethods()).stream()
				.filter(m -> m.getName().equals(setterName))
				.filter(m -> m.getParameterCount() == 1)
				.collect(Collectors.toList());
		if (setters.isEmpty()) {
			return null;
		}
		if (setters.size() > 1) {
			throw new RuntimeException("multiple setters with name [" + setterName + "] and 1 parameter were found");
		}
		return setters.get(0);
	}



	// mapping cache

	private Map<Pair<Resource, Set<Type>>, Object> cachedMappings = new LinkedHashMap<>();

	@Override
	public Object getCachedMapping(Resource resource, Set<Type> targetType) {
		return cachedMappings.get(Pair.of(resource, targetType));
	}

	@Override
	public void addCachedMapping(Resource resource, Set<Type> targetType, Object value) {
		cachedMappings.put(Pair.of(resource, targetType), value);
	}

	private Map<IRI, Type> decidableTypes = new LinkedHashMap<>();

	@Override
	public Type getDecidableType(IRI rdfType) {
		if (!decidableTypes.containsKey(rdfType)) {
			throw new RuntimeException("could not find a java type "
				+ "corresponding to rdf type [" + rdfType + "]");
		}
		return decidableTypes.get(rdfType);
	}

	@Override
	public void addDecidableType(IRI rdfType, Type type) {
		decidableTypes.put(rdfType, type);
	}

	private Map<Type, Type> boundInterfaceImpls = new LinkedHashMap<>();

	@Override
	public void bindInterfaceImplementation(Type interfaze, Type implementation) {
		boundInterfaceImpls.put(interfaze, implementation);
	}

	@Override
	public Type getInterfaceImplementation(Type interfaze) {
		if (!boundInterfaceImpls.containsKey(interfaze)) {
			throw new RuntimeException(String.format("No implementation bound for [%s]", interfaze));
		}

		return boundInterfaceImpls.get(interfaze);
	}

}
