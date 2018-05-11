package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.Value;

public abstract class CarmlMultiTermMap extends CarmlTermMap {

	public CarmlMultiTermMap() {}

	CarmlMultiTermMap(
		String reference,
		String inverseExpression,
		String template,
		TermType termType,
		Value constant,
		TriplesMap functionValue
	) {
		super(reference, inverseExpression, template, termType, constant, functionValue);
	}

	@RdfProperty(Carml.multiReference)
	@Override
	public String getReference() {
		return reference;
	}

	@RdfProperty(Carml.multiTemplate)
	@Override
	public String getTemplate() {
		return template;
	}

	@RdfProperty(Carml.multiFunctionValue)
	@RdfType(CarmlTriplesMap.class)
	@Override
	public TriplesMap getFunctionValue() {
		return functionValue;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

}
