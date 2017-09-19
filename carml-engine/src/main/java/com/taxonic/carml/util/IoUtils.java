package com.taxonic.carml.util;

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

public class IoUtils {

	public static Model parse(String resource) {
		return parse(resource, RDFFormat.TURTLE);
	}
	
	public static Model parseTrig(String resource) {
		return parse(resource, RDFFormat.TRIG);
	}
	
	public static Model parse(String resource, RDFFormat format) {
		try (InputStream input = IoUtils.class.getClassLoader().getResourceAsStream(resource)) {
			return Rio.parse(input, "http://none.com/", format);
		}
		catch (IOException e) {
			throw new RuntimeException("failed to parse resource [" + resource + "] as [" + format + "]", e);
		}
	}
	
	public static Model parse(InputStream input, RDFFormat format) {
		try (InputStream is = input) {
			return Rio.parse(is, "http://none.com/", format);
		}
		catch (IOException e) {
			throw new RuntimeException("failed to parse inputstream [" + input + "] as [" + format + "]", e);
		}
	}
}
