package io.carml.rdfmapper.impl;

import static io.carml.rdfmapper.impl.PropertyUtils.createSetterName;
import static io.carml.rdfmapper.impl.PropertyUtils.findSetter;
import static io.carml.rdfmapper.impl.PropertyUtils.getPropertyName;
import static io.carml.util.ModelSerializer.formatResourceForLog;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.eclipse.rdf4j.model.util.Values.iri;

import com.google.common.collect.Iterables;
import io.carml.rdfmapper.Combiner;
import io.carml.rdfmapper.Mapper;
import io.carml.rdfmapper.PropertyHandler;
import io.carml.rdfmapper.TypeDecider;
import io.carml.rdfmapper.annotations.MultiDelegateCall;
import io.carml.rdfmapper.annotations.RdfProperty;
import io.carml.rdfmapper.annotations.RdfResourceName;
import io.carml.rdfmapper.annotations.RdfType;
import io.carml.rdfmapper.util.Statements;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"java:S1135", "java:S125"})
public class CarmlMapper implements Mapper, MappingCache {

    private static final Logger LOG = LoggerFactory.getLogger(CarmlMapper.class);

    private final Set<Namespace> namespaces;

    private final Map<Pair<Resource, Set<Type>>, Object> cachedMappings = new LinkedHashMap<>();

    private final Map<IRI, Type> decidableTypes = new LinkedHashMap<>();

    private final Map<Type, Type> boundInterfaceImpls = new LinkedHashMap<>();

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

        if (types.size() > 1 && !types.stream().allMatch(t -> ((Class<?>) t).isInterface())) {
            throw new IllegalStateException(String.format(
                    "Error mapping %s. In case of multiple types, mapper requires all types to be interfaces",
                    formatResourceForLog(model, resource, namespaces, true)));
        }

        if (types.stream().allMatch(t -> ((Class<?>) t).isInterface())) {
            return doMultipleInterfaceMapping(model, resource, types);
        } else {
            return doSingleTypeConcreteClassMapping(model, resource, Iterables.getOnlyElement(types));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T doMultipleInterfaceMapping(Model model, Resource resource, Set<Type> types) {
        List<Type> implementations =
                types.stream().map(this::getInterfaceImplementation).toList();

        Map<Type, Object> implementationsToDelegates = implementations.stream()
                .collect(toMap(t -> t, t -> doSingleTypeConcreteClassMapping(model, resource, t)));

        Map<Type, List<Method>> implementationMethods = implementations.stream()
                .collect(toMap(t -> t, t -> gatherMethods((Class<?>) t).toList()));

        BiPredicate<Type, Method> implementationHasMethod =
                (t, m) -> implementationMethods.get(t).contains(m);

        T result = (T) Proxy.newProxyInstance(
                CarmlMapper.class.getClassLoader(), types.toArray(Class<?>[]::new), (proxy, method, args) -> {
                    MultiDelegateCall multiDelegateCall = method.getAnnotation(MultiDelegateCall.class);

                    List<Object> delegates = implementations.stream()
                            .filter(type -> implementationHasMethod.test(type, method))
                            .map(
                                    implementationsToDelegates
                                            ::get) // TODO can this ever be null? this case is not checked below
                            .toList();

                    if (delegates.isEmpty()) {
                        throw new CarmlMapperException(String.format(
                                "Error processing %s%nCould not determine type. (No implementation present with "
                                        + "specified method [%s])",
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
    private Object multiDelegateMethodInvocation(
            List<Object> delegates, Method method, MultiDelegateCall multiDelegateCall, Object... args) {
        Type returnType = method.getReturnType();
        return getCombinerFromMultiDelegateCall(multiDelegateCall)
                .map(combiner -> combiner.combine(delegates.stream()
                        .map(d -> singleDelegateMethodInvocation(d, method, args))
                        .toList()))
                .orElseGet(() -> {
                    if (!returnType.equals(Void.TYPE)) {
                        throw new IllegalStateException(
                                String.format("No combiner specified for non-void multi delegate method %S", method));
                    }
                    delegates.forEach(delegate -> singleDelegateMethodInvocation(delegate, method, args));
                    return null;
                });
    }

    @SuppressWarnings("rawtypes")
    private Optional<Combiner> getCombinerFromMultiDelegateCall(MultiDelegateCall multiDelegateCall) {
        Class<?> combinerClass = multiDelegateCall.value();
        if (combinerClass != null && combinerClass != MultiDelegateCall.Default.class) {
            try {
                return Optional.of((Combiner) combinerClass.getConstructor().newInstance());
            } catch (InstantiationException
                    | IllegalAccessException
                    | IllegalArgumentException
                    | InvocationTargetException
                    | NoSuchMethodException
                    | SecurityException e) {
                throw new CarmlMapperException(
                        String.format(
                                "failed to instantiate multi delegate call combiner class %s",
                                combinerClass.getCanonicalName()),
                        e);
            }
        }
        return Optional.empty();
    }

    private Object singleDelegateMethodInvocation(Object delegate, Method method, Object... args) {
        try {
            return method.invoke(delegate, args);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new CarmlMapperException(
                    String.format("error trying to invoke method [%s] on delegate [%s]", method, delegate), e);
        }
    }

    private Stream<Method> gatherMethods(Class<?> clazz) {
        return concat(
                stream(clazz.getDeclaredMethods()),
                concat(
                        stream(clazz.getInterfaces()).flatMap(this::gatherMethods),
                        Optional.ofNullable(clazz.getSuperclass())
                                .map(this::gatherMethods)
                                .orElse(empty()))
                // .filter(m -> Modifier.isPublic(m.getModifiers()))
                );
    }

    private <T> T doSingleTypeConcreteClassMapping(Model model, Resource resource, Type type) {

        Class<?> clazz = (Class<?>) type;

        if (clazz.isEnum()) {
            throw new CarmlMapperException(String.format(
                    "cannot create an instance of enum type [%s]. you should probably place an instance of the enum"
                            + " type in the MappingCache prior to mapping.",
                    clazz.getCanonicalName()));
        }

        // build meta-model
        // TODO cache

        List<PropertyHandler> propertyHandlers = concat(
                        stream(clazz.getMethods()).flatMap(m -> getRdfPropertyHandlers(m, clazz, model, resource)),
                        stream(clazz.getMethods()).map(m -> getRdfResourceNameHandler(m, clazz)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        Object instance;
        try {
            instance = clazz.getConstructor().newInstance();
        } catch (InstantiationException
                | IllegalAccessException
                | IllegalArgumentException
                | InvocationTargetException
                | NoSuchMethodException
                | SecurityException e) {
            throw new CarmlMapperException(
                    String.format(
                            "Error processing %s%n  failed to instantiate [%s]",
                            formatResourceForLog(model, resource, namespaces, true), clazz.getCanonicalName()),
                    e);
        }

        addCachedMapping(resource, Set.of(type), instance);

        propertyHandlers.forEach(h -> h.handle(model, resource, instance));

        // TODO error if mandatory properties are not present in triples?

        // TODO error if excess/unmapped triples?

        // TODO queue instead of recursion

        @SuppressWarnings("unchecked")
        T result = (T) instance;
        return result;
    }

    private static class PropertyType {

        private final Class<?> elementType;

        private final Class<?> iterableType;

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
        var baseSetter = PropertyUtils.resolveBaseMethod(setter.getDeclaringClass(), setter);

        Type type = baseSetter.getGenericParameterTypes()[0];
        Class<?> elementType = null; // XXX assuming Class<?> for now
        Class<?> iterableType = null;

        // if property type is X<E>, where X <= Iterable, use E as property type from now on
        // XXX not sure if proper place
        if (type instanceof ParameterizedType parameterizedType) {
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            if (Iterable.class.isAssignableFrom(rawType)) {
                elementType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
                iterableType = rawType;
            }
        }

        // otherwise, use property type as-is
        if (elementType == null) {
            elementType = (Class<?>) type;
        }

        return new PropertyType(elementType, iterableType);
    }

    // private PropertyType determinePropertyType(Method setter) {
    //
    //     Type type = setter.getGenericParameterTypes()[0];
    //     Class<?> elementType = null; // XXX assuming Class<?> for now
    //     Class<?> iterableType = null;
    //
    //     var parameterType = GenericsResolver.resolve(setter.getDeclaringClass())
    //             .method(setter)
    //             .resolveParametersTypes()
    //             .get(0);
    //
    //     // if property type is X<E>, where X <= Iterable, use E as property type from now on
    //     // XXX not sure if proper place
    //     if (parameterType instanceof ParameterizedType parameterizedType) {
    //         Class<?> rawType = (Class<?>) parameterizedType.getRawType();
    //         if (Iterable.class.isAssignableFrom(rawType)) {
    //             elementType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
    //             iterableType = rawType;
    //         }
    //     } else if (Iterable.class.isAssignableFrom((Class<?>) parameterType)) {
    //         var superClass = setter.getDeclaringClass().getSuperclass();
    //         if (superClass != null) {
    //             var superSetterOptional = PropertyUtils.findSetter(superClass, setter.getName());
    //             if (superSetterOptional.isPresent()) {
    //                 var superSetter = superSetterOptional.get();
    //                 var superParameterType = GenericsResolver.resolve(superClass)
    //                         .method(superSetter)
    //                         .resolveParametersTypes()
    //                         .get(0);
    //                 if (superParameterType instanceof ParameterizedType superParameterizedType) {
    //                     Class<?> rawType = (Class<?>) superParameterizedType.getRawType();
    //                     if (Iterable.class.isAssignableFrom(rawType)) {
    //                         elementType = (Class<?>) superParameterizedType.getActualTypeArguments()[0];
    //                         iterableType = rawType;
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //
    //     // otherwise, use property type as-is
    //     if (elementType == null) {
    //         elementType = (Class<?>) parameterType;
    //     }
    //
    //     return new PropertyType(elementType, iterableType);
    // }

    private Class<?> getTypeFromRdfTypeAnnotation(Method method) {
        RdfType annotation = method.getAnnotation(RdfType.class);
        if (annotation == null) {
            return null;
        }
        return annotation.value();
    }

    private TypeDecider getTypeDeciderFromAnnotation(Method method) {
        io.carml.rdfmapper.annotations.RdfTypeDecider annotation =
                method.getAnnotation(io.carml.rdfmapper.annotations.RdfTypeDecider.class);
        if (annotation != null) {
            Class<?> deciderClass = annotation.value();
            try {
                return (TypeDecider) deciderClass.getConstructor().newInstance();
            } catch (InstantiationException
                    | IllegalAccessException
                    | IllegalArgumentException
                    | InvocationTargetException
                    | NoSuchMethodException
                    | SecurityException exception) {
                throw new CarmlMapperException(
                        String.format(
                                "failed to instantiate rdf type decider class %s", deciderClass.getCanonicalName()),
                        exception);
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
            return (m, r) -> Set.of(typeFromRdfTypeAnnotation);
        }

        // backup: use this property's type.
        // if that's an interface, use registered implementation thereof, if any.
        // (mapper.registerImplementation(Xyz.class, XyzImpl.class)
        Class<?> implementationType = elementType; // TODO mapper.getRegisteredTypeAlias/Implementation(propertyType);
        // TODO check for pre: no unbound parameter types; not an interface
        TypeDecider propertyTypeDecider = (m, r) -> Set.of(implementationType);

        // use rdf:type triple, if present.
        // => the rdf:type value would have to correspond to an @RdfType annotation on a registered class.
        // TODO probably scan propertyType for @RdfType to register it, here or elsewhere..
        return new TypeFromTripleTypeDecider(this, Optional.of(propertyTypeDecider));
    }

    private Optional<PropertyHandler> getRdfResourceNameHandler(Method method, Class<?> clazz) {
        return Optional.ofNullable(method.getAnnotation(RdfResourceName.class)).map(a -> {
            String name = method.getName();
            String property = getPropertyName(name);

            String setterName = createSetterName(property);
            Method setter = findSetter(clazz, setterName)
                    .orElseThrow(() -> createCouldNotFindSetterException(clazz, setterName));

            BiConsumer<Object, Object> set =
                    getSetterInvoker(setter, createSetterInvocationErrorFactory(clazz, property));

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

    private RuntimeException createCouldNotFindSetterException(Class<?> clazz, String setterName) {
        return new RuntimeException(String.format(
                "in class %s, could not find setter [%s] with 1 parameter", clazz.getCanonicalName(), setterName));
    }

    private Function<Exception, RuntimeException> createSetterInvocationErrorFactory(Class<?> clazz, String property) {
        return e -> new RuntimeException(
                String.format("Unexpected value type for property %s on %s", property, clazz.getSimpleName()), e);
    }

    private BiConsumer<Object, Object> getSetterInvoker(
            Method setter, Function<Exception, RuntimeException> invocationErrorFactory) {
        Objects.requireNonNull(setter);

        // TODO if no setter, set the field directly? (configurable option) ugh

        return (i, v) -> {
            try {
                setter.invoke(i, v);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw invocationErrorFactory.apply(e);
            }
        };
    }

    private Stream<Optional<PropertyHandler>> getRdfPropertyHandlers(
            Method method, Class<?> clazz, Model model, Resource resource) {

        MethodPropertyHandlerRegistry.Builder regBuilder = MethodPropertyHandlerRegistry.builder();

        regBuilder.method(method);
        RdfProperty[] annotations = method.getAnnotationsByType(RdfProperty.class);

        Arrays.stream(annotations)
                .forEach(annotation ->
                        collectRdfPropertyHandler(annotation, method, clazz, regBuilder, model, resource));

        return regBuilder.isBuildable() ? regBuilder.build().getEffectiveHandlers(model, resource) : Stream.empty();
    }

    private void collectRdfPropertyHandler(
            RdfProperty annotation,
            Method method,
            Class<?> clazz,
            MethodPropertyHandlerRegistry.Builder regBuilder,
            Model model,
            Resource resource) {
        String name = method.getName();
        String property = getPropertyName(name);

        String setterName = createSetterName(property);
        Method setter =
                findSetter(clazz, setterName).orElseThrow(() -> createCouldNotFindSetterException(clazz, setterName));

        PropertyType propertyType = determinePropertyType(setter);
        Class<?> elementType = propertyType.getElementType();

        /*
         * TODO valueTransformer is capable of transforming an rdf value to a type expected by this
         * property.
         */
        ValueTransformer valueTransformer;

        // if property type is a sub-type of Value, no transform is needed
        if (Value.class.isAssignableFrom(elementType)) {
            valueTransformer = (inputModel, value) -> value;
        } else if (String.class.isAssignableFrom(elementType)) {
            valueTransformer = (inputModel, value) -> {
                if (value instanceof Literal literal) {
                    return literal.getLabel();
                } else {
                    throw new CarmlMapperException(String.format(
                            "Cannot map value %s for property %s on class %s. Expecting value to be of type Literal, "
                                    + "but was %s",
                            value,
                            annotation.value(),
                            clazz.getSimpleName(),
                            value.getClass().getSimpleName()));
                }
            };
        } else {

            TypeDecider typeDecider = createTypeDecider(method, elementType);

            UnaryOperator<Object> typeAdapter = o -> o;
            // TODO use type adapter from @RdfTypeAdapter annotation, if any

            valueTransformer = new ComplexValueTransformer(typeDecider, this, this, typeAdapter);
        }

        var predicate = iri(annotation.value());

        Class<?> iterableType = propertyType.getIterableType();

        PropertyValueMapper propertyValueMapper;

        boolean iterableProperty = iterableType != null;
        regBuilder.isIterable(iterableProperty);
        // iterable property - set collection value
        if (iterableProperty) {
            var values = model.filter(resource, predicate, null).objects();
            if (values.size() == 1 && List.copyOf(values).get(0) instanceof Resource potentialHead) {
                var collectionType =
                        Statements.getCollectionTypeFor(potentialHead, model.filter(potentialHead, null, null));
                if (collectionType.isPresent()) {
                    propertyValueMapper =
                            RdfCollectionPropertyValueMapper.of(valueTransformer, collectionType.get(), iterableType);
                } else {
                    propertyValueMapper =
                            IterablePropertyValueMapper.createForIterableType(valueTransformer, iterableType);
                }
            } else {
                propertyValueMapper = IterablePropertyValueMapper.createForIterableType(valueTransformer, iterableType);
            }
        } else { // single value property
            propertyValueMapper = new SinglePropertyValueMapper(predicate, valueTransformer);
        }

        BiConsumer<Object, Object> set = getSetterInvoker(setter, createSetterInvocationErrorFactory(clazz, property));

        // get handler from @RdfProperty.handler, if any
        // TODO cache these per type/property
        DefaultPropertyHandlerDependencyResolver dependencyResolver =
                new DefaultPropertyHandlerDependencyResolver(set, predicate, CarmlMapper.this, CarmlMapper.this);
        Optional<PropertyHandler> handler = new SpecifiedPropertyHandlerFactory(new DependencySettersCache())
                .createPropertyHandler(annotation, dependencyResolver);
        if (handler.isPresent()) {
            regBuilder.addHandler(handler.get());
            return;
        }

        registerPropertyHandler(annotation, method, set, predicate, propertyValueMapper, regBuilder);
    }

    private void registerPropertyHandler(
            RdfProperty annotation,
            Method method,
            BiConsumer<Object, Object> set,
            IRI predicate,
            PropertyValueMapper propertyValueMapper,
            MethodPropertyHandlerRegistry.Builder regBuilder) {
        regBuilder.addHandler(new PropertyHandler() {

            @Override
            public void handle(Model model, Resource resource, Object instance) {
                List<Value> values =
                        new ArrayList<>(model.filter(resource, predicate, null).objects());

                if (!values.isEmpty() || propertyValueMapper instanceof IterablePropertyValueMapper) {

                    if (!values.isEmpty() && annotation.deprecated()) {
                        LOG.warn(
                                "Usage of deprecated predicate {} encountered. Support in next release is not "
                                        + "guaranteed. Upgrade to {}.",
                                annotation.value(),
                                getActiveAnnotations(annotation, method));
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
        return Arrays.stream(method.getAnnotationsByType(RdfProperty.class))
                .filter(a -> !a.equals(annotation) && !a.deprecated())
                .map(RdfProperty::value)
                .collect(Collectors.joining(", or "));
    }

    @Override
    public Object getCachedMapping(Resource resource, Set<Type> targetType) {
        return cachedMappings.get(Pair.of(resource, targetType));
    }

    @Override
    public void addCachedMapping(Resource resource, Set<Type> targetType, Object value) {
        cachedMappings.put(Pair.of(resource, targetType), value);
    }

    @Override
    public Type getDecidableType(IRI rdfType) {
        if (!decidableTypes.containsKey(rdfType)) {
            throw new CarmlMapperException(
                    String.format("could not find a java type corresponding to rdf type [%s]", rdfType));
        }
        return decidableTypes.get(rdfType);
    }

    @Override
    public Mapper addDecidableType(IRI rdfType, Type type) {
        decidableTypes.put(rdfType, type);
        return this;
    }

    @Override
    public Mapper bindInterfaceImplementation(Type interfaze, Type implementation) {
        boundInterfaceImpls.put(interfaze, implementation);
        return this;
    }

    @Override
    public Type getInterfaceImplementation(Type interfaze) {
        if (!boundInterfaceImpls.containsKey(interfaze)) {
            throw new CarmlMapperException(String.format("No implementation bound for [%s]", interfaze));
        }

        return boundInterfaceImpls.get(interfaze);
    }
}
