package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.TermType;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
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
		return super.getReference();
	}
	
	@RdfProperty(Carml.multiTemplate)
	@Override
	public String getTemplate() {
		return super.getTemplate();
	}
	
	@RdfProperty(Carml.multiFunctionValue)
	@Override
	public TriplesMap getFunctionValue() {
		return super.getFunctionValue();
	}
	
}
