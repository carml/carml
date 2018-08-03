package com.taxonic.carml.engine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;

/**
 * A crude way of managing sources.
 *
 */
public class LogicalSourceManager {

	Map<String, String> sources;

	public LogicalSourceManager() {
		sources = new HashMap<>();
	}

	public void addSource(String sourceName, InputStream inputStream) {
		try {
			sources.put(sourceName, IOUtils.toString(inputStream, Charset.defaultCharset()));
		} catch (IOException e) {
			throw new RuntimeException(
					String.format("Error while reading inputstream '%s'", sourceName), e);
		}
	}

	public void addSource(String sourceName, String source) {
		sources.put(sourceName, source);
	}

	public String getSource(String sourceName) {
		if (!sources.containsKey(sourceName)) {
			String message =
					sourceName.equals(RmlMapper.DEFAULT_STREAM_NAME) ?
					"attempting to get source, but no binding was present" :
					String.format("attempting to get source by "
							+ "name [%s], but no such binding is present", sourceName);
			throw new RuntimeException(message);
		}

		return sources.get(sourceName);
	}

	public boolean hasSource(String sourceName) {
		return sources.containsKey(sourceName);
	}

	public void clear() {
		sources.clear();
	}

}
