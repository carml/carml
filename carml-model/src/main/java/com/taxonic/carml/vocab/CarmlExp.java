package com.taxonic.carml.vocab;

@SuppressWarnings({"java:S115", "java:S1845"})
public class CarmlExp {

    private CarmlExp() {}

	public static final String PREFIX = "carml-exp";
	public static final String NAMESPACE = Carml.NAMESPACE + "experimental/";

	public static final String context = NAMESPACE + "context";
	public static final String nestedMapping = NAMESPACE + "nestedMapping";
	public static final String subTriplesMap = NAMESPACE + "subTriplesMap";
	public static final String as = NAMESPACE + "as";
	public static final String mergeSuper = NAMESPACE + "mergeSuper";
	public static final String including = NAMESPACE + "including";
	public static final String fromLogicalSource = NAMESPACE + "fromLogicalSource";

}
