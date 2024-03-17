package io.carml.rdfmapper.impl;

import com.google.common.collect.ImmutableMap;
import io.carml.rdfmapper.Mapper;
import io.carml.rdfmapper.TypeDecider;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XSD;

@SuppressWarnings({"java:S1135", "java:S1068"})
public class ComplexValueTransformer implements ValueTransformer {

    private final TypeDecider typeDecider;

    private final MappingCache mappingCache;

    private final Mapper mapper;

    private final Function<Object, Object> typeAdapter;

    public ComplexValueTransformer(
            TypeDecider typeDecider, MappingCache mappingCache, Mapper mapper, UnaryOperator<Object> typeAdapter) {
        this.typeDecider = typeDecider;
        this.mappingCache = mappingCache;
        this.mapper = mapper;
        this.typeAdapter = typeAdapter;
    }

    private static final Map<IRI, Function<Literal, Object>> literalGetters =
            ImmutableMap.<IRI, Function<Literal, Object>>builder()
                    .put(XSD.BOOLEAN, Literal::booleanValue)
                    .put(XSD.STRING, Literal::getLabel)
                    .put(XSD.DECIMAL, Literal::decimalValue)
                    .put(XSD.FLOAT, Literal::floatValue)
                    .put(XSD.INT, Literal::intValue)
                    .put(XSD.INTEGER, Literal::integerValue) // BigInteger
                    .put(XSD.DOUBLE, Literal::doubleValue)
                    // TODO more types, most notably xsd:date and variations
                    .build();

    private Object transform(Literal literal) {
        IRI type = literal.getDatatype();
        Function<Literal, Object> getter = literalGetters.get(type);
        if (getter == null) {
            throw new CarmlMapperException(String.format(
                    "no getter for Literal [%s] defined that can handle literal with datatype [%s]", literal, type));
        }
        return getter.apply(literal);
    }

    @Override
    public Object transform(Model model, Value value) {

        if (value instanceof Literal literal) {
            return transform(literal);
        }

        // =========== RESOURCE ===========

        Resource resource = (Resource) value;

        // determine exact target type
        Set<Type> targetTypes = typeDecider.decide(model, resource);

        // TODO check for target type conditions?
        // - must be a subtype of 'propertyType'
        // - must be a specific type, eg. no unbound type parameters, not an interface

        Object targetValue = mapper.map(model, resource, targetTypes);

        // TODO check cache for adapted value (key: typeAdapter + targetValue)

        // TODO maybe we should cache this as well, in a diff. cache. (key: typeAdapter + targetValue)
        return typeAdapter.apply(targetValue);
    }
}
