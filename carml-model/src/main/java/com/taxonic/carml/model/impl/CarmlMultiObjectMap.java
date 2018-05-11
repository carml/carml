package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.MultiObjectMap;
import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rr;
import java.util.Objects;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

public class CarmlMultiObjectMap extends CarmlMultiTermMap implements MultiObjectMap {

	private IRI datatype;
	private String language;

	public CarmlMultiObjectMap() {}

	CarmlMultiObjectMap(
		String reference,
		String inverseExpression,
		String template,
		TermType termType,
		Value constant,
		TriplesMap functionValue,
		IRI datatype,
		String language
	) {
		super(reference, inverseExpression, template, termType, constant, functionValue);
		this.datatype = datatype;
		this.language = language;
	}

	@RdfProperty(Rr.datatype)
	@Override
	public IRI getDatatype() {
		return datatype;
	}

	@RdfProperty(Rr.language)
	@Override
	public String getLanguage() {
		return language;
	}

	public void setDatatype(IRI datatype) {
		this.datatype = datatype;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(datatype, language, super.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CarmlMultiObjectMap other = (CarmlMultiObjectMap) obj;
		return Objects.equals(datatype, other.datatype) && Objects.equals(language, other.language);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder
		extends CarmlTermMap.Builder {

		private IRI datatype;
		private String language;

		public Builder datatype(IRI datatype) {
			this.datatype = datatype;
			return this;
		}

		public Builder language(String language) {
			this.language = language;
			return this;
		}

		public CarmlMultiObjectMap build() {
			return new CarmlMultiObjectMap(
				getReference(),
				getInverseExpression(),
				getTemplate(),
				getTermType(),
				getConstant(),
				getFunctionValue(),
				datatype,
				language
			);
		}
	}

}
