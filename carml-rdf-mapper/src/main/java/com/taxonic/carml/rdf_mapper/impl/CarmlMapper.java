package com.taxonic.carml.rdf_mapper.impl;


import static com.taxonic.carml.rdf_mapper.impl.PropertyUtils.createSetterName;
import static com.taxonic.carml.rdf_mapper.impl.PropertyUtils.findSetter;
import static com.taxonic.carml.rdf_mapper.impl.PropertyUtils.getPropertyName;
import static com.taxonic.carml.util.ModelSerializer.formatResourceForLog;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.taxonic.carml.rdf_mapper.Combiner;
import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.PropertyHandler;
import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.rdf_mapper.annotations.MultiDelegateCall;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfResourceName;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CarmlMapper implements Mapper, MappingCache {

	private static final Logger LOG = LoggerFactory.getLogger(CarmlMapper.class);

	private Set<Namespace> namespaces;

	public CarmlMapper() {
		this(new HashSet<>());
	}
	public CarmlMapper(Set<Namespace> namespaces) {
		this.namespaces = namespaces;
	}

	// type is the exact type of instance we need; eg. NOT a supertype.
	// so no unbound type parameters. no interface, unless a list or so.
	// TODO *could* be an interface, but then an implementation type should be registered.
	@Override
	@SuppressWarnings("unchecked")
	public <T> T map(Model model, Resource resource, Set<Type> types) {

		// before mapping, first check the cache for an existing mapping
		// NOTE: cache includes pre-mapped/registered enum instances
		// such as <#Male> -> Gender.Male for property gender : Gender
		Object cached = getCachedMapping(resource, types);
		if (cached != null) {
			return (T) cached;
		}

		if (types.size() > 1) {
			if (!types.stream().allMatch(t -> ((Class<?>)t).isInterface())) {
				throw new IllegalStateException(String.format(
						"Error mapping %s. In case of multiple types, mapper requires all types to be interfaces",
						formatResourceForLog(model, resource, namespaces, true)));
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


		T result = (T) Proxy.newProxyInstance(CarmlMapper.class.getClassLoader(), types.stream().toArray(Class<?>[]::new),
				(proxy, method, args) -> {

					MultiDelegateCall multiDelegateCall = method.getAnnotation(MultiDelegateCall.class);

					List<Object> delegates = implementations.stream()
							.filter(t -> implementationHasMethod.apply(t, method))
							.map(implementationsToDelegates::get) // TODO can this ever be null? this case is not checked below
							.collect(toList());

					if (delegates.isEmpty()) {
						throw new RuntimeException(String.format(
								"Error processing %s%nCould not determine type. (No implementation present with specified method [%s])",
										formatResourceForLog(model, resource, namespaces, true), method));
					}

					if (multiDelegateCall != null) {
						return multiDelegateMethodInvocation(delegates, method, multiDelegateCall, args);
					} else {
						return delegates.stream()
								.findFirst()
								.map(d -> singleDelegateMethodInvocation(d, method, args))
								// since we know 'delegate' is present, the case
								// here means the invoked method returned null. so we
								// return null.
								.orElse(null);
					}
				});
		addCachedMapping(resource, types, result);

		return result;
	}

	@SuppressWarnings("unchecked")
	private Object multiDelegateMethodInvocation(List<Object> delegates, Method method,
			MultiDelegateCall multiDelegateCall, Object... args) {
		Type returnType = method.getReturnType();
		return getCombinerFromMultiDelegateCall(multiDelegateCall)
				.map(combiner -> combiner.combine(delegates
								.stream()
								.map(d -> singleDelegateMethodInvocation(d, method, args))
								.collect(toList()))
				)
				.orElseGet(() -> {
					if (!returnType.equals(Void.TYPE)) {
						throw new IllegalStateException(String.format(
								"No combiner specified for non-void multi delegate method %S", method));
					}
					return null;
				});
	}

	private Optional<Combiner> getCombinerFromMultiDelegateCall(MultiDelegateCall multiDelegateCall) {
		Class<?> combinerClass = multiDelegateCall.value();
		if (combinerClass != null && combinerClass != MultiDelegateCall.DEFAULT.class) {
			try {
				return Optional.of((Combiner) combinerClass.getConstructor().newInstance());
			} catch (
					InstantiationException |
							IllegalAccessException |
							IllegalArgumentException |
							InvocationTargetException |
							NoSuchMethodException |
							SecurityException e
			) {
				throw new RuntimeException(String.format(
						"failed to instantiate multi delegate call combiner class %s", combinerClass.getCanonicalName()), e);
			}
		}
		return Optional.empty();
	}

	private Object singleDelegateMethodInvocation(Object delegate, Method method, Object... args) {
		try {
		  return method.invoke(delegate, args);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new RuntimeException(
					String.format(
							"error trying to invoke method [%s] on delegate [%s]",
							method, delegate), e);
		}
	}



	private Stream<Method> gatherMethods(Class<?> clazz) {
		return concat(
				stream(clazz.getDeclaredMethods()),
				concat(
						stream(clazz.getInterfaces()).flatMap(this::gatherMethods),
						Optional.ofNullable(clazz.getSuperclass())
							.map(this::gatherMethods)
							.orElse(empty())
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
				concat(
						stream(c.getMethods())
						.flatMap(m -> getRdfPropertyHandlers(m, c, model, resource))
				,
						stream(c.getMethods())
						.map(m -> getRdfResourceNameHandler(m, c))
				)
				.filter(Optional::isPresent).map(Optional::get)
				.collect(toList());

		Object instance;
		try {
			instance = c.getConstructor().newInstance();
		}
		catch (
				InstantiationException |
				IllegalAccessException |
				IllegalArgumentException |
				InvocationTargetException |
				NoSuchMethodException |
				SecurityException e
		) {
			throw new RuntimeException(String.format("Error processing %s%n  failed to instantiate [%s]"
					, formatResourceForLog(model, resource, namespaces, true), c.getCanonicalName()), e);
		}

		addCachedMapping(resource, ImmutableSet.of(type), instance);

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
				return (TypeDecider) deciderClass.getConstructor().newInstance();
			}
			catch (
					InstantiationException |
					IllegalAccessException |
					IllegalArgumentException |
					InvocationTargetException |
					NoSuchMethodException |
					SecurityException e
			) {
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

			return new PropertyHandler() {
				@Override
				public void handle(Model model, Resource resource, Object instance) {
					set.accept(instance, resource.stringValue());
				}

				@Override
				public boolean hasEffect(Model model, Resource resource) {
					return true;
				}
			};
		});
	}
	
	private RuntimeException createCouldNotFindSetterException(Class<?> c, String setterName) {
		return new RuntimeException(String.format("in class %s, could not find setter [%s] with 1 parameter",
				c.getCanonicalName(), setterName));
	}

	private Function<Exception, RuntimeException> createSetterInvocationErrorFactory(Class<?> c, Method setter) {
		return e -> new RuntimeException(String.format("could not invoke setter [%s.%s]", c.getSimpleName(), setter.getName()), e);
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

	private Stream<Optional<PropertyHandler>> getRdfPropertyHandlers(Method method, Class<?> c, Model model, Resource resource) {

		MethodPropertyHandlerRegistry.Builder regBuilder = MethodPropertyHandlerRegistry.builder();

		regBuilder.method(method);
		RdfProperty[] annotations = method.getAnnotationsByType(RdfProperty.class);

		Arrays.asList(annotations) //
				.stream() //
				.forEach(a -> collectRdfPropertyHandler(a, method, c, regBuilder));

		return regBuilder.isBuildable() ? regBuilder.build().getEffectiveHandlers(model, resource) : Stream.empty();
	}

	private void collectRdfPropertyHandler(RdfProperty annotation, Method method, Class<?> c, MethodPropertyHandlerRegistry.Builder regBuilder) {
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

		Boolean iterableProperty = iterableType != null;
		regBuilder.isIterable(iterableProperty);
		// iterable property - set collection value
		if (iterableProperty) {
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
			new SpecifiedPropertyHandlerFactory(new DependencySettersCache())
						.createPropertyHandler(annotation, dependencyResolver);
		if (handler.isPresent()) {
			regBuilder.addHandler(handler.get());
			return;
		}

		regBuilder.addHandler(new PropertyHandler() {

			@Override
			public void handle(Model model, Resource resource, Object instance) {
				List<Value> values =
					model.filter(resource, predicate, null)
						.objects()
						.stream()
						.collect(toList());

				if (!values.isEmpty() || propertyValueMapper instanceof IterablePropertyValueMapper) {

					if (!values.isEmpty() && annotation.deprecated()) {
						LOG.warn(
								"Usage of deprecated predicate {} encountered. Support in next release is not guaranteed. Upgrade to {}.",
								annotation.value(), getActiveAnnotations(annotation, method));
					}

					// map data from rdf model to a value for this property,
					// such as a list of complex objects, or a simple string.
					propertyValueMapper.map(model, resource, instance, values).ifPresent(v -> set.accept(instance, v));

					// TODO what about languages? @RdfLanguage("nl") to select only 1 language (or
					// multiple?)
					// then, if multiple such values exist in the graph, the property should be a
					// List<String>.
				}
				// no values
				// TODO error if property has @RdfRequired?
			}

			@Override
			public boolean hasEffect(Model model, Resource resource) {
				return !model.filter(resource, predicate, null).isEmpty();
			}
		});
	}

	private static String getActiveAnnotations(RdfProperty annotation, Method method) {
		return Arrays.asList(method.getAnnotationsByType(RdfProperty.class)) //
				.stream()//
				.filter(a -> !a.equals(annotation) && !a.deprecated()) //
				.map(RdfProperty::value) //
				.collect(Collectors.joining(", or "));
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
			throw new RuntimeException(String.format("could not find a java type corresponding to rdf type [%s]", rdfType));
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
