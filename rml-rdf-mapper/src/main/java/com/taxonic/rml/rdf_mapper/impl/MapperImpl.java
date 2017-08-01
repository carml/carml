package com.taxonic.rml.rdf_mapper.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.taxonic.rml.rdf_mapper.Mapper;
import com.taxonic.rml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.rdf_mapper.annotations.RdfType;
import com.taxonic.rml.rdf_mapper.util.PropertyUtils;

public class MapperImpl implements Mapper, MappingCache {

	// type is the exact type of instance we need; eg. NOT a supertype.
	// so no unbound type parameters. no interface, unless a list or so.
	// TODO *could* be an interface, but then an implementation type should be registered.
	@Override
	public <T> T map(Model model, Resource resource, Type type) {
		
		
		// assuming class without type parameters for now
		Class<?> c = (Class<?>) type;
		
		// build meta-model
		// TODO cache
		List<PropertyHandler> propertyHandlers =
			Arrays.asList(c.getMethods()).stream()
				.map(m -> getRdfPropertyHandler(m, c))
				.filter(h -> h != null)
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
	
	private static interface PropertyHandler {
		
		void handle(Model model, Resource resource, Object instance);
		
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
		if (elementType == null)
			elementType = (Class<?>) type;
		
		return new PropertyType(elementType, iterableType);
	}
	
	private Class<?> getTypeFromRdfTypeAnnotation(Method method) {
		RdfType annotation = method.getAnnotation(RdfType.class);
		if (annotation == null) return null;
		return annotation.value();
	}
	
	private TypeDecider createTypeDecider(Method method, Class<?> elementType) {

		// TODO if @RdfTypeDecider(Xyz.class) is present on property, use that
		
		// if @RdfType(MyImpl.class) is present on property, use that
		Class<?> typeFromRdfTypeAnnotation = getTypeFromRdfTypeAnnotation(method);
		if (typeFromRdfTypeAnnotation != null)
			return (m, r) -> typeFromRdfTypeAnnotation;

		// backup: use this property's type.
		// if that's an interface, use registered implementation thereof, if any.
		// (mapper.registerImplementation(Xyz.class, XyzImpl.class)
		Class<?> implementationType = elementType; // TODO mapper.getRegisteredTypeAlias/Implementation(propertyType);
		// TODO check for pre: no unbound parameter types; not an interface
		TypeDecider propertyTypeDecider = (m, r) -> implementationType;

		// use rdf:type triple, if present.
		//   => the rdf:type value would have to correspond to an @RdfType annotation on a registered class.
		//      TODO probably scan propertyType for @RdfType to register it, here or elsewhere..
		return new TypeFromTripleTypeDecider(propertyTypeDecider);
		
	}
	
	private Supplier<Collection<Object>> createCollectionFactory(Class<?> iterableType) {
		if (iterableType == null) return null;
		// TODO Map<Class<?>, Supplier<Collection<Object>>>
		if (iterableType.equals(Set.class))
			return LinkedHashSet::new;
		else if (iterableType.equals(List.class))
			return LinkedList::new;
		throw new RuntimeException("don't know how to create a factory for collection type [" + iterableType.getCanonicalName() + "]");
	}
	
	private PropertyHandler getRdfPropertyHandler(Method method, Class<?> c) {
		
		RdfProperty annotation = method.getAnnotation(RdfProperty.class);
		if (annotation == null) return null;
		
		String name = method.getName();
		String property = PropertyUtils.getPropertyName(name);
		
		String setterName = PropertyUtils.createSetterName(property);
		Method setter = findSetter(c, setterName);
		// TODO if no setter, set the field directly? (configurable option) ugh
		if (setter == null)
			throw new RuntimeException("could not find setter [" + setterName + "] with 1 parameter");
		BiConsumer<Object, Object> set = (i, v) -> {
			try {
//				System.out.println("invoking [" + c.getSimpleName() + "." + setter.getName() + "]");
				setter.invoke(i, v);
			}
			catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RuntimeException("could not invoke setter [" + c.getSimpleName() + "." + setter.getName() + "]", e);
			}
		};
		
		PropertyType propertyType = determinePropertyType(setter);
		Class<?> elementType = propertyType.getElementType();
		
		/* TODO valueTransformer is capable of transforming an rdf value
		 * to a type expected by this property.
		 */
		ValueTransformer valueTransformer;
		
		// if property type is a sub-type of Value, no transform is needed
		if (Value.class.isAssignableFrom(elementType))
			valueTransformer = (m, v) -> v;
		
		// if property is a string, get Literal label
		// TODO @RdfTypeAdapter should be possible here as well. maybe the other stuff below as well?
		else if (String.class.isAssignableFrom(elementType))
			valueTransformer = (m, v) -> ((Literal) v).getLabel();
		
		// TODO more primitive types
		
		// complex type
		else {
			
			TypeDecider typeDecider = createTypeDecider(method, elementType);
			
			Function<Object, Object> typeAdapter = o -> o;
			// TODO use type adapter from @RdfTypeAdapter annotation, if any
			
			valueTransformer = new ComplexValueTransformer(typeDecider, this, this, typeAdapter);
		}
		
		ValueFactory f = SimpleValueFactory.getInstance();
		IRI predicate = f.createIRI(annotation.value());
		
		Class<?> iterableType = propertyType.getIterableType();
		Supplier<Collection<Object>> createIterable = createCollectionFactory(iterableType);
		
		// TODO use @RdfProperty.handler, if any, instead of the default impl of PropertyHandler below
		
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
					set.accept(instance, result);
					
				}
				
				// single value property
				else {
					
					// multiple values present - error
					if (values.size() > 1)
						throw new RuntimeException("multiple values for property [" + predicate + "], but "
							+ "corresponding java property is NOT an Iterable property");
					
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
		if (setters.isEmpty()) return null;
		if (setters.size() > 1)
			throw new RuntimeException("multiple setters with name [" + setterName + "] and 1 parameter were found");
		return setters.get(0);
	}
	
	
	
	// mapping cache

	private Map<Pair<Resource, Type>, Object> cachedMappings = new LinkedHashMap<>();
	
	@Override
	public Object getCachedMapping(Resource resource, Type targetType) {
		return cachedMappings.get(Pair.of(resource, targetType));
	}
	
	@Override
	public void addCachedMapping(Resource resource, Type targetType, Object value) {
		cachedMappings.put(Pair.of(resource, targetType), value);
	}	
	
}
