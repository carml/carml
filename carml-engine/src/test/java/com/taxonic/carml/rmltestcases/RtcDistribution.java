package com.taxonic.carml.rmltestcases;

import java.net.URL;

import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rmltestcases.model.Distribution;

public class RtcDistribution extends RtcResource implements Distribution {

	private URL downloadUrl;

	@RdfProperty("http://www.w3.org/ns/dcat#downloadUrl")
	@Override
	public URL getDownloadUrl() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setDownloadUrl(URL downloadUrl) {
		this.downloadUrl = downloadUrl;
	}

	@Override
	public String getRelativeFileLocation() {
		String urlString = downloadUrl.toString();
		return urlString.substring(urlString.lastIndexOf("/rml-test-cases/master/"));
	}

}
