package io.carml.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
    private static final IRI VALUE_PARAM_IRI = VF.createIRI(GREL + "valueParam");
    private static final IRI STRING_OUT_RESOURCE_IRI = VF.createIRI(GREL + "stringOut");

    private static final IRI CONCAT_IRI = VF.createIRI(GREL + "concat");
    private static final IRI LEFT_PARAM_IRI = VF.createIRI(GREL + "leftParam");
    private static final IRI RIGHT_PARAM_IRI = VF.createIRI(GREL + "rightParam");

    // -- Test function class --

    @SuppressWarnings("unused") // methods are discovered reflectively by FnoDescriptionProvider
    public static class TestFunctions {

        public String toUpperCase(String value) {
            return value.toUpperCase();
        }

        public String concat(String left, String right) {
            return left + right;
        }
    }

    /** A class with only a parameterized constructor (no default constructor). */
    @SuppressWarnings("unused") // methods are discovered reflectively by FnoDescriptionProvider
    public static class NoDefaultConstructorFunctions {

        private final String config;

        public NoDefaultConstructorFunctions(String config) {
            this.config = config;
        }

        public String doSomething(String input) {
            return config + input;
        }
    }

    /** A utility class with static methods only (no instance state). */
    @SuppressWarnings("unused") // methods are discovered reflectively by FnoDescriptionProvider
    public static class StaticUtilityFunctions {

        private StaticUtilityFunctions() {
            // intentional: should not be instantiated
            throw new UnsupportedOperationException("static utility class");
        }

        public static String shout(String input) {
            return input.toUpperCase() + "!";
        }

        public static String join(String left, String right) {
            return left + "-" + right;
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
        assertThat(descriptor.getParameters().get(0).parameterIri(), is(VALUE_PARAM_IRI));
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
        assertThat(descriptor.getParameters().get(0).parameterIri(), is(LEFT_PARAM_IRI));
        assertThat(descriptor.getParameters().get(1).parameterIri(), is(RIGHT_PARAM_IRI));
    }

    @Test
    void getFunctions_extractsReturnDescriptor_givenFnoOutput() {
        var model = createSingleFunctionModel();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor.getReturns(), hasSize(1));
        var ret = descriptor.getReturns().get(0);
        assertThat(ret.outputIri(), is(STRING_OUT_RESOURCE_IRI));
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
    void getFunctions_skipsFunction_givenMissingClass() {
        // Per FnoDescriptionProvider's "leniency for unknown functions" policy, a function whose
        // implementation class is not on the classpath is logged and skipped rather than failing
        // the whole load — so other functions in the same model continue to register.
        var model = createModelWithMissingClass();

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_skipsFunction_givenMissingMethod() {
        // Same leniency rationale as getFunctions_skipsFunction_givenMissingClass.
        var model = createModelWithMissingMethod();

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
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
    void getFunctions_skipsFunction_givenClassWithNoDefaultConstructor() {
        // Same leniency rationale as getFunctions_skipsFunction_givenMissingClass: an
        // un-instantiable class is logged and skipped rather than aborting the whole load.
        var model = createModelWithNoDefaultConstructorClass();

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_returnsDescriptorWithDefaultReturn_givenNoFnoReturns() {
        var model = createFunctionModelWithoutReturns();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor, is(notNullValue()));
        assertThat(descriptor.getReturns(), hasSize(1));
        assertThat(descriptor.getReturns().get(0).outputIri(), is(nullValue()));
        assertThat(descriptor.getReturns().get(0).type(), is(Object.class));
    }

    @Test
    void getFunctions_setsParameterPredicateIri_givenFnoPredicate() {
        var predicateIri = VF.createIRI(GREL + "valuePredicate");
        var model = createSingleFunctionModelWithPredicate(predicateIri);

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        var paramDesc = descriptor.getParameters().get(0);
        assertThat(paramDesc.parameterIri(), is(VALUE_PARAM_IRI));
        assertThat(paramDesc.predicateIri(), is(predicateIri));
        assertThat(paramDesc.matches(predicateIri), is(true));
        assertThat(paramDesc.matches(VALUE_PARAM_IRI), is(true));
    }

    @Test
    void execute_invokesMethodWithReorderedArgs_givenPositionParameterMapping() {
        // Function declares (left, right) but Position mapping says left → slot 1 and right → slot 0,
        // so calling concat with leftValue="A", rightValue="B" should produce "BA".
        var model = createConcatModelWithSwappedPositions();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        var result = descriptor.execute(Map.of(LEFT_PARAM_IRI, "A", RIGHT_PARAM_IRI, "B"));

        assertThat(result, is("BA"));
    }

    @Test
    void getFunctions_supportsStaticMethod_givenClassWithoutDefaultConstructor() {
        var model = createStaticMethodModel();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        var result = descriptor.execute(Map.of(VALUE_PARAM_IRI, "hello"));

        assertThat(result, is("HELLO!"));
    }

    @Test
    void getFunctions_derivesDescriptorFromMethodReflection_givenMappingWithoutFnoFunction() {
        var model = createMappingOnlyModel();

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        // No fno:Function declaration: parameters are synthesized from the Java method signature.
        assertThat(descriptor.getParameters(), hasSize(1));
        var paramDesc = descriptor.getParameters().get(0);
        assertThat(paramDesc.type(), is(String.class));
        assertThat(paramDesc.parameterIri().stringValue(), is(TO_UPPER_IRI.stringValue() + "#param-0"));
        assertThat(descriptor.getReturns(), hasSize(1));
        assertThat(descriptor.getReturns().get(0).type(), is(String.class));

        var result = descriptor.execute(Map.of(paramDesc.parameterIri(), "hi"));

        assertThat(result, is("HI"));
    }

    @Test
    void getFunctions_setsReturnOutputIri_givenDefaultReturnMapping() {
        var stringOutputIri = VF.createIRI(GREL + "stringOutput");
        var model = createMappingWithDefaultReturnMapping(stringOutputIri);

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor.getReturns(), hasSize(1));
        assertThat(descriptor.getReturns().get(0).outputIri(), is(stringOutputIri));
    }

    // -- Negative-path tests for fno:Mapping validation --

    @Test
    void getFunctions_skipsFunction_givenDuplicatePosition() {
        // Two parameters mapped to the same position 0.
        var model = createConcatModelWithPositions(0, 0);

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_skipsFunction_givenOutOfRangePosition() {
        // 2-parameter function but a position references slot 5.
        var model = createConcatModelWithPositions(0, 5);

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_skipsFunction_givenMissingPositionForOneParameter() {
        // Position is given for left only; right is unbound.
        var model = createMultiParamFunctionModel();
        var mapping = VF.createIRI(EX + "concatMapping");
        var leftPm = VF.createBNode();
        model.add(leftPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(leftPm, Fnom.functionParameter, LEFT_PARAM_IRI);
        model.add(leftPm, Fnom.implementationParameterPosition, VF.createLiteral(0));
        model.add(mapping, Fno.parameterMapping, leftPm);

        var provider = new FnoDescriptionProvider(model);

        // Only position 0 is given for a 2-arg method; contiguity check rejects (n=1, slot 0 only)
        // and the upstream method-arity filter then can't match — descriptor skipped.
        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_skipsFunction_givenNonIntegerPositionLiteral() {
        var model = createMultiParamFunctionModel();
        var mapping = VF.createIRI(EX + "concatMapping");
        var pm = VF.createBNode();
        model.add(pm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(pm, Fnom.functionParameter, LEFT_PARAM_IRI);
        model.add(pm, Fnom.implementationParameterPosition, VF.createLiteral("not-a-number"));
        model.add(mapping, Fno.parameterMapping, pm);

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_skipsFunction_givenMissingFunctionParameterOnPositionMapping() {
        var model = createMultiParamFunctionModel();
        var mapping = VF.createIRI(EX + "concatMapping");
        var pm = VF.createBNode();
        model.add(pm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(pm, Fnom.implementationParameterPosition, VF.createLiteral(0));
        // fnom:functionParameter intentionally omitted.
        model.add(mapping, Fno.parameterMapping, pm);

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_skipsFunction_givenDuplicateFunctionParameterMapping() {
        // Same fno:Parameter referenced by two PositionParameterMappings — fnom:toMap merge fires.
        var model = createMultiParamFunctionModel();
        var mapping = VF.createIRI(EX + "concatMapping");
        var pm1 = VF.createBNode();
        model.add(pm1, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(pm1, Fnom.functionParameter, LEFT_PARAM_IRI);
        model.add(pm1, Fnom.implementationParameterPosition, VF.createLiteral(0));
        model.add(mapping, Fno.parameterMapping, pm1);

        var pm2 = VF.createBNode();
        model.add(pm2, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(pm2, Fnom.functionParameter, LEFT_PARAM_IRI);
        model.add(pm2, Fnom.implementationParameterPosition, VF.createLiteral(1));
        model.add(mapping, Fno.parameterMapping, pm2);

        var provider = new FnoDescriptionProvider(model);

        assertThat(provider.getFunctions(), is(empty()));
    }

    @Test
    void getFunctions_supportsCombinedStaticPositionAndReturnMapping() {
        // The three new features stacked: static method on a class with no public no-arg
        // constructor + position swap + DefaultReturnMapping bound to a specific output IRI.
        var leftParam = LEFT_PARAM_IRI;
        var rightParam = RIGHT_PARAM_IRI;
        var stringOutputIri = VF.createIRI(GREL + "joinOutput");
        var model = createCombinedStaticPositionReturnModel(leftParam, rightParam, stringOutputIri);

        var provider = new FnoDescriptionProvider(model);
        var descriptor = provider.getFunctions().iterator().next();

        assertThat(descriptor.getReturns(), hasSize(1));
        assertThat(descriptor.getReturns().get(0).outputIri(), is(stringOutputIri));

        // Position map says left -> slot 1, right -> slot 0; static join concatenates with a dash:
        // join(slot0, slot1) = join(rightValue, leftValue) -> "B-A".
        var result = descriptor.execute(Map.of(leftParam, "A", rightParam, "B"));

        assertThat(result, is("B-A"));
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

        model.add(valueParam, Fno.type, XSD.STRING);
        model.add(valueParam, Fno.required, VF.createLiteral(true));

        // Output resource
        var stringOut = VF.createIRI(GREL + "stringOut");
        model.add(stringOut, RDF.TYPE, Fno.Output);

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
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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

        model.add(leftParam, Fno.type, XSD.STRING);
        model.add(leftParam, Fno.required, VF.createLiteral(true));

        // Right parameter
        var rightParam = VF.createIRI(GREL + "rightParam");
        model.add(rightParam, RDF.TYPE, Fno.Parameter);

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
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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

        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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

        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList1 = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList1);

        var javaClass1 = VF.createIRI(EX + "javaClass1");
        model.add(javaClass1, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass1,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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

        model.add(leftParam, Fno.type, XSD.STRING);

        var rightParam = VF.createIRI(GREL + "rightParam");
        model.add(rightParam, RDF.TYPE, Fno.Parameter);

        model.add(rightParam, Fno.type, XSD.STRING);

        var expectsList2 = buildRdfList(model, leftParam, rightParam);

        model.add(CONCAT_IRI, RDF.TYPE, Fno.Function);
        model.add(CONCAT_IRI, Fno.expects, expectsList2);

        var javaClass2 = VF.createIRI(EX + "concatJavaClass");
        model.add(javaClass2, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass2,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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

        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$NoDefaultConstructorFunctions"));

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

    /**
     * Like {@link #createSingleFunctionModel()} but the parameter resource carries an
     * {@code fno:predicate} so the provider should populate {@code predicateIri}.
     */
    private Model createSingleFunctionModelWithPredicate(IRI predicateIri) {
        var model = createSingleFunctionModel();
        model.add(VALUE_PARAM_IRI, Fno.predicate, predicateIri);
        return model;
    }

    /**
     * concat(left, right) but the Mapping has explicit Position bindings that swap the
     * conceptual order (left → slot 1, right → slot 0).
     */
    private Model createConcatModelWithSwappedPositions() {
        var model = createMultiParamFunctionModel();

        var mapping = VF.createIRI(EX + "concatMapping");

        var leftPm = VF.createBNode();
        model.add(leftPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(leftPm, Fnom.functionParameter, LEFT_PARAM_IRI);
        model.add(leftPm, Fnom.implementationParameterPosition, VF.createLiteral(1));
        model.add(mapping, Fno.parameterMapping, leftPm);

        var rightPm = VF.createBNode();
        model.add(rightPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(rightPm, Fnom.functionParameter, RIGHT_PARAM_IRI);
        model.add(rightPm, Fnom.implementationParameterPosition, VF.createLiteral(0));
        model.add(mapping, Fno.parameterMapping, rightPm);

        return model;
    }

    /**
     * Mapping bound to {@code StaticUtilityFunctions.shout} — a static method on a class with
     * a private constructor that throws on instantiation.
     */
    private Model createStaticMethodModel() {
        var model = new TreeModel();

        var valueParam = VF.createIRI(GREL + "valueParam");
        model.add(valueParam, RDF.TYPE, Fno.Parameter);
        model.add(valueParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, valueParam);

        model.add(TO_UPPER_IRI, RDF.TYPE, Fno.Function);
        model.add(TO_UPPER_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$StaticUtilityFunctions"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("shout"));

        var mapping = VF.createIRI(EX + "mapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, TO_UPPER_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        return model;
    }

    /**
     * Mapping with no {@code fno:Function} declaration — descriptors must be derived from the
     * resolved Java method's reflected signature.
     */
    private Model createMappingOnlyModel() {
        var model = new TreeModel();

        var javaClass = VF.createIRI(EX + "javaClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$TestFunctions"));

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
     * Multi-param concat mapping with explicit positions for both parameters. Used by negative
     * tests for duplicate / out-of-range position values.
     */
    private Model createConcatModelWithPositions(int leftPosition, int rightPosition) {
        var model = createMultiParamFunctionModel();
        var mapping = VF.createIRI(EX + "concatMapping");

        var leftPm = VF.createBNode();
        model.add(leftPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(leftPm, Fnom.functionParameter, LEFT_PARAM_IRI);
        model.add(leftPm, Fnom.implementationParameterPosition, VF.createLiteral(leftPosition));
        model.add(mapping, Fno.parameterMapping, leftPm);

        var rightPm = VF.createBNode();
        model.add(rightPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(rightPm, Fnom.functionParameter, RIGHT_PARAM_IRI);
        model.add(rightPm, Fnom.implementationParameterPosition, VF.createLiteral(rightPosition));
        model.add(mapping, Fno.parameterMapping, rightPm);

        return model;
    }

    /**
     * Mapping that exercises three features at once: static method on a no-default-ctor class,
     * {@code fnom:PositionParameterMapping} reordering, and {@code fnom:DefaultReturnMapping}.
     */
    private Model createCombinedStaticPositionReturnModel(IRI leftParam, IRI rightParam, IRI outputIri) {
        var model = new TreeModel();

        model.add(leftParam, RDF.TYPE, Fno.Parameter);
        model.add(leftParam, Fno.type, XSD.STRING);
        model.add(rightParam, RDF.TYPE, Fno.Parameter);
        model.add(rightParam, Fno.type, XSD.STRING);

        var expectsList = buildRdfList(model, leftParam, rightParam);

        model.add(CONCAT_IRI, RDF.TYPE, Fno.Function);
        model.add(CONCAT_IRI, Fno.expects, expectsList);

        var javaClass = VF.createIRI(EX + "staticJoinClass");
        model.add(javaClass, RDF.TYPE, Fnoi.JavaClass);
        model.add(
                javaClass,
                Fnoi.class_name,
                VF.createLiteral("io.carml.functions.FnoDescriptionProviderTest$StaticUtilityFunctions"));

        var methodMapping = VF.createBNode();
        model.add(methodMapping, RDF.TYPE, Fnom.StringMethodMapping);
        model.add(methodMapping, Fnom.method_name, VF.createLiteral("join"));

        var mapping = VF.createIRI(EX + "joinMapping");
        model.add(mapping, RDF.TYPE, Fno.Mapping);
        model.add(mapping, Fno.function, CONCAT_IRI);
        model.add(mapping, Fno.implementation, javaClass);
        model.add(mapping, Fno.methodMapping, methodMapping);

        // left -> slot 1, right -> slot 0
        var leftPm = VF.createBNode();
        model.add(leftPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(leftPm, Fnom.functionParameter, leftParam);
        model.add(leftPm, Fnom.implementationParameterPosition, VF.createLiteral(1));
        model.add(mapping, Fno.parameterMapping, leftPm);

        var rightPm = VF.createBNode();
        model.add(rightPm, RDF.TYPE, Fnom.PositionParameterMapping);
        model.add(rightPm, Fnom.functionParameter, rightParam);
        model.add(rightPm, Fnom.implementationParameterPosition, VF.createLiteral(0));
        model.add(mapping, Fno.parameterMapping, rightPm);

        // DefaultReturnMapping binding the function output to a specific IRI
        model.add(outputIri, RDF.TYPE, Fno.Output);
        model.add(outputIri, Fno.type, XSD.STRING);
        var returnMapping = VF.createBNode();
        model.add(returnMapping, RDF.TYPE, Fnom.DefaultReturnMapping);
        model.add(returnMapping, Fnom.functionOutput, outputIri);
        model.add(mapping, Fno.returnMapping, returnMapping);

        return model;
    }

    private Model createMappingWithDefaultReturnMapping(IRI outputIri) {
        var model = createSingleFunctionModel();

        var mapping = VF.createIRI(EX + "mapping");

        model.add(outputIri, RDF.TYPE, Fno.Output);
        model.add(outputIri, Fno.type, XSD.STRING);

        var returnMapping = VF.createBNode();
        model.add(returnMapping, RDF.TYPE, Fnom.DefaultReturnMapping);
        model.add(returnMapping, Fnom.functionOutput, outputIri);
        model.add(mapping, Fno.returnMapping, returnMapping);

        return model;
    }

    private static Resource buildRdfList(Model model, IRI... elements) {
        var head = VF.createBNode();
        RDFCollections.asRDF(List.of(elements), head, model);
        return head;
    }
}
