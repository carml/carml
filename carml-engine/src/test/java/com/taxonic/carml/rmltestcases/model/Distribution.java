package com.taxonic.carml.rmltestcases.model;

import java.net.URL;

public interface Distribution extends Resource {

	URL getDownloadUrl();

	String getRelativeFileLocation();

}
