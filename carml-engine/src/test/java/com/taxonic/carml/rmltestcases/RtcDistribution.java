package com.taxonic.carml.rmltestcases;

import com.taxonic.carml.rdfmapper.annotations.RdfProperty;
import com.taxonic.carml.rmltestcases.model.Distribution;
import org.eclipse.rdf4j.model.IRI;

public class RtcDistribution extends RtcResource implements Distribution {

  private IRI downloadUrl;

  @RdfProperty("http://www.w3.org/ns/dcat#downloadUrl")
  @RdfProperty("http://www.w3.org/ns/dcat#downloadURL")
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
    return urlString.substring(urlString.lastIndexOf("master/") + 7);
  }

}
