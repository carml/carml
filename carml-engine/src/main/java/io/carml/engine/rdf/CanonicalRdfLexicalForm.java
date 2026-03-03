package io.carml.engine.rdf;

import io.carml.rdfmapper.impl.CarmlMapperException;
import jakarta.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.function.BiFunction;
import lombok.NoArgsConstructor;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;

@NoArgsConstructor(staticName = "get")
public final class CanonicalRdfLexicalForm implements BiFunction<Object, IRI, String> {

    @Override
    public String apply(Object value, IRI datatype) {
        try {
            if (datatype != null) {
                if (datatype.equals(XSD.HEXBINARY) && value instanceof ByteBuffer byteBuffer) {
                    return DatatypeConverter.printHexBinary(byteBuffer.array());
                } else if (value instanceof TemporalAccessor temporalAccessor) {
                    // MySQL R2DBC wraps TIMESTAMP values in ZonedDateTime with the session
                    // timezone, but TIMESTAMP is semantically timezone-unaware. Strip the zone
                    // to produce timezone-free xsd:dateTime canonical form.
                    if (temporalAccessor instanceof ZonedDateTime zdt) {
                        temporalAccessor = zdt.toLocalDateTime();
                    }
                    var lexicalForm = Values.literal(temporalAccessor).stringValue();
                    return XMLDatatypeUtil.normalize(lexicalForm, datatype);
                } else if (CoreDatatype.from(datatype).isXSDDatatype()) {
                    return XMLDatatypeUtil.normalize(value.toString(), datatype);
                }
            }
        } catch (RuntimeException exception) {
            throw new CarmlMapperException(
                    "Failed to normalize value '%s' with datatype '%s'".formatted(value, datatype), exception);
        }

        return value.toString();
    }
}
