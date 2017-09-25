package com.taxonic.carml.util;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class IoUtilsTest {
	
	@Test
	public void readAndResetInputStream_givenInputStream_shouldReadInputStreamRepeatedly() {
		String string = 
				"foo\r\n" + 
				"bar\r\n" + 
				"milk\r\n" + 
				"dud";
		
		InputStream inputStream = IOUtils.toInputStream(string);
		
		assertThat(IoUtils.readAndResetInputStream(inputStream), is(string));
		
		//re-read input stream
		assertThat(IoUtils.readAndResetInputStream(inputStream), is(string));
	}

}
