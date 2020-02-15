package com.taxonic.carml.rmltestcases;

import org.eclipse.rdf4j.model.IRI;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rmltestcases.model.Distribution;

public class RtcDistribution extends RtcResource implements Distribution {

	private IRI downloadUrl;

	@RdfProperty("http://www.w3.org/ns/dcat#downloadUrl")
	@Override
	public IRI getDownloadUrl() {
		return downloadUrl;
	}

	public void setDownloadUrl(IRI downloadUrl) {
		this.downloadUrl = downloadUrl;
	}

	@Override
	public String getRelativeFileLocation() {
		String urlString = downloadUrl.stringValue();
		return urlString.substring(urlString.lastIndexOf("test-cases/"));
	}

}
