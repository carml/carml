package com.taxonic.carml.util;

import org.eclipse.rdf4j.common.net.ParsedIRI;

import java.net.URISyntaxException;

public class RdfUtil {

    public static boolean isValidIri(String str) {
        if (!str.contains(":")) {
            return false;
        }
        try {
            return new ParsedIRI(str).getScheme() != null;
        } catch (URISyntaxException uriException) {
            return false;
        }
    }

}
