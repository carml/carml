package com.taxonic.carml.rdf_mapper.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import javax.inject.Inject;

import org.eclipse.rdf4j.model.IRI;

import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.qualifiers.PropertyPredicate;
import com.taxonic.carml.rdf_mapper.qualifiers.PropertySetter;

/** Simple {@code DependencyResolver} that contains, and can resolve,
 * (optional) dependencies for custom property handlers, set through
 * {@link RdfProperty#handler()}.
 * 
 * Any such property handler is instantiated
 * through its (no-args) constructor, and can then have things injected
 * through setters annotated with {@link Inject}. Such injections are
 * attempted to be resolved by an instance of this class.
 * 
 * The current implementation simply contains several values that can be
 * injected by type or by using a qualifier. Things that can currently be
 * injected by type: {@link Mapper}, {@link MappingCache}. Things that can
 * be injected by pre-set qualifiers: a {@code BiConsumer&lt;Object, Object&gt;},
 * which takes the mapped instance and value to set for the property as arguments,
 * qualified by {@link PropertySetter}, the predicate of the property, of type
 * {@link IRI}, qualified by {@link PropertyPredicate}.
 */
class DefaultPropertyHandlerDependencyResolver implements DependencyResolver {

	private BiConsumer<Object, Object> set;
	private IRI predicate;
	private Mapper mapper;
	private MappingCache mappingCache;
	
	public DefaultPropertyHandlerDependencyResolver(
		BiConsumer<Object, Object> set,
		IRI predicate,
		Mapper mapper,
		MappingCache mappingCache
	) {
		this.set = set;
		this.predicate = predicate;
		this.mapper = mapper;
		this.mappingCache = mappingCache;
	}

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
			return Optional.of(mapper);
		} else if (type.equals(MappingCache.class)) {
			return Optional.of(mappingCache);
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
