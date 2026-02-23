package io.carml.engine.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.function.FnoDescriptionProvider.FnoDescriptionException;
import io.carml.vocab.Rdf.Fno;
import io.carml.vocab.Rdf.Fnoi;
import io.carml.vocab.Rdf.Fnom;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class FnoDescriptionProviderTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String EX = "http://example.org/";
    private static final String GREL = "http://example.org/grel/";

    private static final IRI TO_UPPER_IRI = VF.createIRI(GREL + "toUpperCase");
    private static final IRI VALUE_PARAM_IRI = VF.createIRI(GREL + "valueParameter");
    private static final IRI STRING_OUTPUT_IRI = VF.createIRI(GREL + "stringOutput");

    private static final IRI CONCAT_IRI = VF.createIRI(GREL + "concat");
    private static final IRI LEFT_PARAM_IRI = VF.createIRI(GREL + "leftParameter");
    private static final IRI RIGHT_PARAM_IRI = VF.createIRI(GREL + "rightParameter");

    // -- Test function class --

    public static class TestFunctions {

        public String toUpperCase(String value) {
            return value.toUpperCase();
        }

        public String concat(String left, String right) {
            return left + right;
        }
    }

    /** A class with only a parameterized constructor (no default constructor). */
    public static class NoDefaultConstructorFunctions {

        private final String config;

        public NoDefaultConstructorFunctions(String config) {
            this.config = config;
        }

        public String doSomething(String input) {
            return config + input;
        }
    }

    // -- Tests --

    @Test
    void getFunctions_returnsDescriptor_givenValidFnoDescriptionAndMapping() {
        var model = createSingleFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var functions = provider.getFunctions();

        assertThat(functions, hasSize(1));

        var descriptor = functions.iterator().next();
        assertThat(descriptor.getFunctionIri(), is(TO_UPPER_IRI));
        assertThat(descriptor.getParameters(), hasSize(1));
        assertThat(descriptor.getParameters().get(0).iri(), is(VALUE_PARAM_IRI));
        assertThat(descriptor.getParameters().get(0).type(), is(String.class));
        assertThat(descriptor.getParameters().get(0).required(), is(true));
    }

    @Test
    void getFunctions_extractsParametersInOrder_givenMultipleParams() {
        var model = createMultiParamFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var functions = provider.getFunctions();

        assertThat(functions, hasSize(1));

        var descriptor = functions.iterator().next();
        assertThat(descriptor.getParameters(), hasSize(2));
        assertThat(descriptor.getParameters().get(0).iri(), is(LEFT_PARAM_IRI));
        assertThat(descriptor.getParameters().get(1).iri(), is(RIGHT_PARAM_IRI));
    }

    @Test
    void getFunctions_extractsReturnDescriptor_givenFnoOutput() {
        var model = createSingleFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor.getReturns(), hasSize(1));
        var ret = descriptor.getReturns().get(0);
        assertThat(ret.iri(), is(STRING_OUTPUT_IRI));
        assertThat(ret.type(), is(String.class));
    }

    @Test
    void getFunctions_handlesOptionalParam_givenRequiredFalse() {
        var model = createFunctionModelWithOptionalParam();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor.getParameters(), hasSize(1));
        assertThat(descriptor.getParameters().get(0).required(), is(false));
    }

    @Test
    void getFunctions_throwsException_givenMissingClass() {
        var model = createModelWithMissingClass();

        assertThrows(FnoDescriptionException.class, () -> new FnoDescriptionProvider(model));
    }

    @Test
    void getFunctions_throwsException_givenMissingMethod() {
        var model = createModelWithMissingMethod();

        assertThrows(FnoDescriptionException.class, () -> new FnoDescriptionProvider(model));
    }

    @Test
    void getFunctions_returnsEmptyCollection_givenNoFunctions() {
        var model = new TreeModel();

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void execute_invokesMethod_givenBoundParameters() {
        var model = createSingleFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        var result = descriptor.execute(Map.of(VALUE_PARAM_IRI, "hello"));

        assertThat(result, is("HELLO"));
    }

    @Test
    void getFunctions_skipsFunction_givenNoMapping() {
        var model = createFunctionModelWithoutMapping();

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void execute_invokesMultiParamMethod_givenBoundParameters() {
        var model = createMultiParamFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        var result = descriptor.execute(Map.of(LEFT_PARAM_IRI, "foo", RIGHT_PARAM_IRI, "bar"));

        assertThat(result, is("foobar"));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdString() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.STRING), is(String.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdInteger() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.INTEGER), is(Integer.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdInt() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.INT), is(Integer.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdBoolean() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.BOOLEAN), is(Boolean.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdDouble() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.DOUBLE), is(Double.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdFloat() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.FLOAT), is(Float.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdLong() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.LONG), is(Long.class));
    }

    @Test
    void mapXsdType_returnsCorrectJavaType_givenXsdDecimal() {
        assertThat(FnoDescriptionProvider.mapXsdType(XSD.DECIMAL), is(Double.class));
    }

    @Test
    void mapXsdType_returnsObjectClass_givenUnknownType() {
        assertThat(FnoDescriptionProvider.mapXsdType(VF.createIRI("http://example.org/unknown")), is(Object.class));
    }

    @Test
    void mapXsdType_returnsListClass_givenRdfList() {
        assertThat(FnoDescriptionProvider.mapXsdType(RDF.LIST), is(List.class));
    }

    @Test
    void mapXsdType_returnsObjectClass_givenXsdAny() {
        // xsd:any is not a standard XSD type; verify it falls through to the default Object.class
        assertThat(FnoDescriptionProvider.mapXsdType(VF.createIRI(XSD.NAMESPACE + "any")), is(Object.class));
    }

    @Test
    void getFunctions_returnsBothDescriptors_givenTwoFunctionsInOneModel() {
        var model = createTwoFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var functions = provider.getFunctions();

        assertThat(functions, hasSize(2));
    }

    @Test
    void getFunctions_throwsException_givenClassWithNoDefaultConstructor() {
        var model = createModelWithNoDefaultConstructorClass();

        assertThrows(FnoDescriptionException.class, () -> new FnoDescriptionProvider(model));
    }

    @Test
    void getFunctions_returnsDescriptorWithDefaultReturn_givenNoFnoReturns() {
        var model = createFunctionModelWithoutReturns();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor, is(notNullValue()));
        assertThat(descriptor.getReturns(), hasSize(1));
        assertThat(descriptor.getReturns().get(0).iri(), is(nullValue()));
        assertThat(descriptor.getReturns().get(0).type(), is(Object.class));
    }

    // -- Model builders --

    /**
     * Creates an RDF model with a single fno:Function (toUpperCase) with one parameter,
     * one return, and a valid fno:Mapping to TestFunctions.toUpperCase.
     */
    private Model createSingleFunctionModel() {
        var model = new TreeModel();

        // Parameter resource
        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);
        model.add(valueParam, Fno.required, VF.createLiteral(true));

        // Output resource
        var stringOut = VF.createIRI(GREL + "stringOut");
        model.add(stringOut, RDF.TYPE, Fno.Output);
        model.add(stringOut, Fno.predicate, STRING_OUTPUT_IRI);
        model.add(stringOut, Fno.type, XSD.STRING);

        // Build expects RDF list (single element)
        var expectsList = buildRdfList(model, valueParam);

        // Build returns RDF list (single element)
        var returnsList = buildRdfList(model, stringOut);

        // Function
        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);
        model.add(TO_UPPER_IRI, Fno.returns, returnsList);

        // Implementation
        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        // Method mapping
        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("toUpperCase"));

        // Mapping
        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Creates an RDF model with a concat function that has two parameters,
     * verifying list ordering.
     */
    private Model createMultiParamFunctionModel() {
        var model = new TreeModel();

        // Left parameter
        var leftParam = VF.createIRI(GREL + "leftParam");
        model.add(leftParam, RDF.TYPE, Fno.Parameter);
        model.add(leftParam, Fno.predicate, LEFT_PARAM_IRI);
        model.add(leftParam, Fno.type, XSD.STRING);
        model.add(leftParam, Fno.required, VF.createLiteral(true));

        // Right parameter
        var rightParam = VF.createIRI(GREL + "rightParam");
        model.add(rightParam, RDF.TYPE, Fno.Parameter);
        model.add(rightParam, Fno.predicate, RIGHT_PARAM_IRI);
        model.add(rightParam, Fno.type, XSD.STRING);
        model.add(rightParam, Fno.required, VF.createLiteral(true));

        // Build expects list (order matters: left, right)
        var expectsList = buildRdfList(model, leftParam, rightParam);

        // Function
        model.add(CONCAT_IRI, RDF.TYPE, Fno.Function);
        model.add(CONCAT_IRI, Fno.expects, expectsList);

        // Implementation
        var javaClass = VF.createIRI(EX + "concatJavaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        // Method mapping
        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("concat"));

        // Mapping
        var mapping = VF.createIRI(EX + "concatMapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, CONCAT_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Creates a model with an optional parameter (required = false).
     */
    private Model createFunctionModelWithOptionalParam() {
        var model = new TreeModel();

        // Optional parameter
        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);
        model.add(valueParam, Fno.required, VF.createLiteral(false));

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        // Implementation
        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("toUpperCase"));

        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Creates a model where the implementation class does not exist.
     */
    private Model createModelWithMissingClass() {
        var model = new TreeModel();

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(javaClass, Fnoi.class_name, VF.createLiteral("com.example.DoesNotExist"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("toUpperCase"));

        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Creates a model where the method name does not exist on the target class.
     */
    private Model createModelWithMissingMethod() {
        var model = new TreeModel();

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("nonExistentMethod"));

        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Creates a model with a function description but no fno:Mapping binding.
     */
    private Model createFunctionModelWithoutMapping() {
        var model = new TreeModel();

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        return model;
    }

    /**
     * Creates a model with a function that has no fno:returns.
     */
    private Model createFunctionModelWithoutReturns() {
        var model = new TreeModel();

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        // Implementation + mapping (no returns on function)
        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("toUpperCase"));

        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Creates an RDF model containing two functions (toUpperCase and concat) in a single model.
     */
    private Model createTwoFunctionModel() {
        var model = new TreeModel();

        // -- toUpperCase function --

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList1 = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList1);

        var javaClass1 = VF.createIRI(EX + "javaClass1");
        model.add(javaClass1, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass1,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        var methodMapping1 = VF.createBNode();
        model.add(methodMapping1, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping1, Fnom.method_name, VF.createLiteral("toUpperCase"));

        var mapping1 = VF.createIRI(EX + "mapping1");
        model.add(mapping1, RDF.TYPE, Fno.Mapping);
        model.add(mapping1, Fno.function, TO_UPPER_IRI);
        model.add(mapping1, Fno.implementation, javaClass1);
        model.add(mapping1, Fno.methodMapping, methodMapping1);

        // -- concat function --

        var leftParam = VF.createIRI(GREL + "leftParam");
        model.add(leftParam, RDF.TYPE, Fno.Parameter);
        model.add(leftParam, Fno.predicate, LEFT_PARAM_IRI);
        model.add(leftParam, Fno.type, XSD.STRING);

        var rightParam = VF.createIRI(GREL + "rightParam");
        model.add(rightParam, RDF.TYPE, Fno.Parameter);
        model.add(rightParam, Fno.predicate, RIGHT_PARAM_IRI);
        model.add(rightParam, Fno.type, XSD.STRING);

        var expectsList2 = buildRdfList(model, leftParam, rightParam);

        model.add(CONCAT_IRI, RDF.TYPE, Fno.Function);
        model.add(CONCAT_IRI, Fno.expects, expectsList2);

        var javaClass2 = VF.createIRI(EX + "concatJavaClass");
        model.add(javaClass2, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass2,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$TestFunctions"));

        var methodMapping2 = VF.createBNode();
        model.add(methodMapping2, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping2, Fnom.method_name, VF.createLiteral("concat"));

        var mapping2 = VF.createIRI(EX + "concatMapping");
        model.add(mapping2, RDF.TYPE, Fno.Mapping);
        model.add(mapping2, Fno.function, CONCAT_IRI);
        model.add(mapping2, Fno.implementation, javaClass2);
        model.add(mapping2, Fno.methodMapping, methodMapping2);

        return model;
    }

    /**
     * Creates a model where the implementation class has no default constructor.
     */
    private Model createModelWithNoDefaultConstructorClass() {
        var model = new TreeModel();

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.predicate, VALUE_PARAM_IRI);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.engine.function.FnoDescriptionProviderTest$NoDefaultConstructorFunctions"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("doSomething"));

        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    private static Resource buildRdfList(Model model, IRI... elements) {
        var head = VF.createBNode();
        RDFCollections.asRDF(List.of(elements), head, model);
        return head;
    }
}
