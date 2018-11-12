package com.taxonic.carml.rdf_mapper.impl;

import static com.taxonic.carml.rdf_mapper.impl.PropertyUtils.createSetterName;
import static com.taxonic.carml.rdf_mapper.impl.PropertyUtils.findSetter;
import static com.taxonic.carml.rdf_mapper.impl.PropertyUtils.getPropertyName;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.PropertyHandler;
import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfResourceName;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;

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
		List<Type> implementations = types.stream().map(this::getInterfaceImplementation).collect(toList());

		Map<Type, Object> implementationsToDelegates = implementations.stream()
				.collect(toMap(
					t -> t,
					t-> doSingleTypeConcreteClassMapping(model, resource, t)));

		Map<Type, List<Method>> implementationMethods =
				implementations.stream().collect(toMap(
						t -> t,
						t -> gatherMethods((Class<?>) t).collect(toList())));

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
				,
						Arrays.asList(c.getMethods()).stream()
						.map(m -> getRdfResourceNameHandler(m, c))
				)
				.filter(Optional::isPresent).map(Optional::get)
				.collect(toList());

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


	private Optional<PropertyHandler> getRdfResourceNameHandler(Method method, Class<?> c) {
		return
		Optional.ofNullable(method.getAnnotation(RdfResourceName.class))
		.map(a -> {

			String name = method.getName();
			String property = getPropertyName(name);

			String setterName = createSetterName(property);
			Method setter = findSetter(c, setterName)
				.orElseThrow(() -> createCouldNotFindSetterException(c, setterName));

			BiConsumer<Object, Object> set = getSetterInvoker(setter, createSetterInvocationErrorFactory(c, setter));

			return (Model model, Resource resource, Object instance) -> {
				set.accept(instance, resource.stringValue());
			};
		});
	}
	
	private RuntimeException createCouldNotFindSetterException(Class<?> c, String setterName) {
		return new RuntimeException("in class " + c.getCanonicalName() + ", could not find setter [" + setterName + "] with 1 parameter");
	}

	private Function<Exception, RuntimeException> createSetterInvocationErrorFactory(Class<?> c, Method setter) {
		return e -> new RuntimeException("could not invoke setter [" + c.getSimpleName() + "." + setter.getName() + "]", e);
	}
	
	private BiConsumer<Object, Object> getSetterInvoker(Method setter, Function<Exception, RuntimeException> invocationErrorFactory) {
		Objects.requireNonNull(setter);
		
		// TODO if no setter, set the field directly? (configurable option) ugh
		
		return (i, v) -> {
			try {
				setter.invoke(i, v);
			}
			catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw invocationErrorFactory.apply(e);
			}
		};
	}

	private Optional<PropertyHandler> getRdfPropertyHandler(Method method, Class<?> c) {

		RdfProperty annotation = method.getAnnotation(RdfProperty.class);
		if (annotation == null) {
			return Optional.empty();
		}

		String name = method.getName();
		String property = getPropertyName(name);

		String setterName = createSetterName(property);
		Method setter = findSetter(c, setterName)
				.orElseThrow(() -> createCouldNotFindSetterException(c, setterName));

		BiConsumer<Object, Object> set = getSetterInvoker(setter, createSetterInvocationErrorFactory(c, setter));

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
		
		PropertyValueMapper propertyValueMapper;
		
		// iterable property - set collection value
		if (iterableType != null) {
			propertyValueMapper = IterablePropertyValueMapper
					.createForIterableType(valueTransformer, iterableType);
		}
		
		// single value property
		else {
			propertyValueMapper = new SinglePropertyValueMapper(predicate, valueTransformer);			
		}
		
		// get handler from @RdfProperty.handler, if any
		// TODO cache these per type/property
		DefaultPropertyHandlerDependencyResolver dependencyResolver =
			new DefaultPropertyHandlerDependencyResolver(set, predicate, CarmlMapper.this, CarmlMapper.this);
		Optional<PropertyHandler> handler =
			new SpecifiedPropertyHandlerFactory().createPropertyHandler(annotation, dependencyResolver);
		if (handler.isPresent()) {
			return handler;
		}

		return Optional.of(new PropertyHandler() {

			@Override
			public void handle(Model model, Resource resource, Object instance) {

				List<Value> values =
					model.filter(resource, predicate, null)
						.objects()
						.stream()
						.collect(toList());

				// no values
				if (values.isEmpty()) {
					// TODO error if property has @RdfRequired?
				}

				// map data from rdf model to a value for this property,
				// such as a list of complex objects, or a simple string.
				propertyValueMapper.map(model, instance, values)
					.ifPresent(v -> set.accept(instance, v));

				// TODO what about languages? @RdfLanguage("nl") to select only 1 language (or multiple?)
				// then, if multiple such values exist in the graph, the property should be a List<String>.

			}
		});
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
