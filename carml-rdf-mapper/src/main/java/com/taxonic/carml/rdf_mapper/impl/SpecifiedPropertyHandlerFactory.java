package com.taxonic.carml.rdf_mapper.impl;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.taxonic.carml.rdf_mapper.PropertyHandler;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;

/** An instance of this class is used to create an instance of a
 * custom property handler, based on an annotation on a getter or setter.
 * This class facilitates also performs some dependency injection on
 * the instantiated handler, so that custom code may have access to
 * the current {@code Mapper} instance, for example.
 *
 * @see RdfProperty
 * @see DependencyResolver
 */
public class SpecifiedPropertyHandlerFactory {

	public Optional<PropertyHandler> createPropertyHandler(
		RdfProperty annotation,
		DependencyResolver resolver
	) {

		if (annotation.handler().equals(PropertyHandler.class)) {
			return Optional.empty();
		}

		Class<? extends PropertyHandler> handlerCls = annotation.handler();
		
		PropertyHandler handler = createInstance(handlerCls);

		injectDependencies(handlerCls, resolver, handler);

		return Optional.of(handler);
	}
	
	private <T extends PropertyHandler> T createInstance(Class<T> handlerCls) {
		try {
			return handlerCls.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("could not instantiate specified "
				+ "PropertyHandler class [" + handlerCls.getCanonicalName() + "]");
		}
	}
	
	private void injectDependencies(Class<?> cls, DependencyResolver resolver, Object instance) {

		// do dependency injection through setter methods annotated with @Inject
		Arrays.asList(cls.getMethods()).stream()

			// find methods annotated with @Inject
			.filter(m -> m.getAnnotation(Inject.class) != null)
			
			// for each such setter, create a consumer that will take a
			// handler instance, and will resolve and set the correct
			// dependency.
			.map(m ->
				createDependencySetter(
					m,
					resolver,
					getPropertyType(m),
					getPropertyQualifiers(m)
				)
			)
			
			.forEach(i -> i.accept(instance));
		
	}

	private Type getPropertyType(Method method) {
		List<Type> parameterTypes = asList(method.getGenericParameterTypes());
		if (parameterTypes.isEmpty() || parameterTypes.size() > 1) {
			throw new RuntimeException("method [" + method.getName() + "], annotated "
				+ "with @Inject does NOT take exactly 1 parameter; it takes " + parameterTypes.size());
		}
		return parameterTypes.get(0);
	}
	
	private boolean isQualifierInstance(Annotation annotation) {
		return annotation.annotationType().getAnnotation(Qualifier.class) != null;
	}
	
	private List<Annotation> getPropertyQualifiers(Method method) {
		return
		asList(method.getAnnotations()).stream()
			.filter(this::isQualifierInstance)
			.collect(toList());
	}

	private Consumer<Object> createDependencySetter(
		Method method,
		DependencyResolver resolver,
		Type propertyType,
		List<Annotation> qualifiers
	) {
		return
		i -> {
			
			// resolve dependency through resolver, which may use the property
			// type and qualifiers, if any.
			Object propertyValue = resolver.resolve(propertyType, qualifiers);
			try {
				
				// invoke the setter to set the resolved value
				method.invoke(i, propertyValue);
				
			}
			catch (
				IllegalAccessException |
				IllegalArgumentException |
				InvocationTargetException e
			) {
				throw new RuntimeException("error invoking setter [" + method.getName() + "]", e);
			}
		};
	}
	
}
