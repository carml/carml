package com.taxonic.rml.engine;

import static org.hamcrest.CoreMatchers.startsWith;
import java.io.InputStream;
import java.util.function.Function;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.taxonic.rml.engine.template.TemplateParser;

public class ExceptionsTest extends MappingTest {
	
	
    @Rule 
    public ExpectedException thrown = ExpectedException.none();
	
    @Test
    public void testInvalidIRIGeneratorException() {
    	thrown.expect(RuntimeException.class);
    	thrown.expectMessage(startsWith("data error: could not generate a vald iri from term lexical form"));
		testMapping("RmlMapper/exceptionTests/exceptionInvalidIRIMapping.rml.ttl",
				"RmlMapper", null);
    }
    
    @Test
    public void testGeneratorMappingException() {
    	thrown.expect(RuntimeException.class);
    	thrown.expectMessage(startsWith("could not create generator for map"));
		testMapping("RmlMapper/exceptionTests/exceptionGeneratorMappping.rml.ttl",
				"RmlMapper", null);
    }
    
    @Test
    public void testUnknownTermTypeException() {
    	thrown.expect(RuntimeException.class);
    	thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper/exceptionTests/exceptionUnknownTermTypeMapping.rml.ttl",
				"RmlMapper", null);
    }
    
    @Test
    public void testConstantValueException() {
    	thrown.expect(RuntimeException.class);
    	thrown.expectMessage(startsWith("encountered constant value of type"));
		testMapping("RmlMapper/exceptionTests/exceptionConstantValueMapping.rml.ttl",
				"RmlMapper", null);
    }
    
    @Test
    public void testEqualLogicalSourceException() {
    	thrown.expect(RuntimeException.class);
    	thrown.expectMessage(startsWith("Logical sources are not equal."));
		testMapping("RmlMapper/exceptionTests/exceptionEqualLogicalSourceMapping.rml.ttl",
				"RmlMapper", null);
    }
    
	@Test
	public void testReadSourceException() throws RuntimeException{
		Function<String, InputStream> sourceResolver =
				s -> RmlMapperTest.class.getClassLoader().getResourceAsStream("RmlMapper" + "/" + s);
		RmlMapper tm = new RmlMapper(sourceResolver, TemplateParser.build());
		//TODO change readSource back to private.
		String result = tm.readSource("exceptions.json");
		System.out.println(result);
	}
	
	@Test
	public void testTermTypeExceptionA() {
		thrown.expect(RuntimeException.class);
        thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper/exceptionTests/exceptionTermTypeMappingA.rml.ttl",
				"RmlMapper", null);
	}
	
	@Test
	public void testTermTypeExceptionB() {
		thrown.expect(RuntimeException.class);
        thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper/exceptionTests/exceptionTermTypeMappingB.rml.ttl",
				"RmlMapper", null);
	}
	
	@Test
	public void testTermTypeExceptionC() {
		thrown.expect(RuntimeException.class);
        thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper/exceptionTests/exceptionTermTypeMappingC.rml.ttl",
				"RmlMapper", null);
	}
	
	@Test
	public void testTermTypeExceptionD() {
		thrown.expect(RuntimeException.class);
        thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper/exceptionTests/exceptionTermTypeMappingD.rml.ttl",
				"RmlMapper", null);
	}
	
	@Test
	public void testTermTypeExceptionE() {
		thrown.expect(RuntimeException.class);
        thrown.expectMessage(startsWith("encountered disallowed term type"));
		testMapping("RmlMapper/exceptionTests/exceptionTermTypeMappingE.rml.ttl",
				"RmlMapper", null);
	}
	

	}

