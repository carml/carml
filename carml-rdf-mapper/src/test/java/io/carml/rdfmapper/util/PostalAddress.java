package io.carml.rdfmapper.util;

import io.carml.rdfmapper.annotations.RdfProperty;

public class PostalAddress {

  private String streetAddress;

  private String addressLocality;

  private String addressRegion;

  private String postalCode;

  static final String SCHEMAORG = "http://schema.org/";

  static final String SCHEMAORG_STREETADDRESS = SCHEMAORG + "streetAddress";

  static final String SCHEMAORG_ADDRESSLOCALITY = SCHEMAORG + "addressLocality";

  static final String SCHEMAORG_ADDRESSREGION = SCHEMAORG + "addressRegion";

  static final String SCHEMAORG_POSTALCODE = SCHEMAORG + "postalCode";

  @RdfProperty(SCHEMAORG_STREETADDRESS)
  public String getStreetAddress() {
    return streetAddress;
  }

  public void setStreetAddress(String streetAddress) {
    this.streetAddress = streetAddress;
  }

  @RdfProperty(SCHEMAORG_ADDRESSLOCALITY)
  public String getAddressLocality() {
    return addressLocality;
  }

  public void setAddressLocality(String addressLocality) {
    this.addressLocality = addressLocality;
  }

  @RdfProperty(SCHEMAORG_ADDRESSREGION)
  public String getAddressRegion() {
    return addressRegion;
  }

  public void setAddressRegion(String addressRegion) {
    this.addressRegion = addressRegion;
  }

  @RdfProperty(SCHEMAORG_POSTALCODE)
  public String getPostalCode() {
    return postalCode;
  }

  public void setPostalCode(String postalCode) {
    this.postalCode = postalCode;
  }

  @Override
  public String toString() {
    return "PostalAddress [streetAddress=" + streetAddress + ", addressLocality=" + addressLocality + ", addressRegion="
        + addressRegion + ", postalCode=" + postalCode + "]";
  }
}
