package com.taxonic.carml.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
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
	
	public static Model parse(InputStream inputStream, RDFFormat format) {
		try (InputStream is = inputStream) {
			return Rio.parse(is, "http://none.com/", format);
		}
		catch (IOException e) {
			throw new RuntimeException("failed to parse input stream [" + inputStream + "] as [" + format + "]", e);
		}
	}
	
	public static String readAndResetInputStream(InputStream inputStream) throws RuntimeException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		int length;
		try {
			inputStream.mark(0);
			while ((length = inputStream.read(buffer)) != -1) {
			    result.write(buffer, 0, length);
			}
			inputStream.reset();
		} catch (IOException e) {
			throw new RuntimeException(String.format("failed to read input stream [%s]", inputStream), e);
		}
		try {
			return result.toString("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("failed stringifying input stream", e);
		}
	}
}
