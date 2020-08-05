package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Resource;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfResourceName;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDFS;

public abstract class CarmlResource implements Resource {

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	String id;
	String label;
	private Map<Resource, Model> modelCache = new HashMap<>();

	@Override
	@RdfResourceName
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	@RdfProperty("http://www.w3.org/2000/01/rdf-schema#label")
	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public org.eclipse.rdf4j.model.Resource getAsResource() {
		if (id.startsWith("http://") || id.startsWith("https://")) {
			return VF.createIRI(id);
		}
		return VF.createBNode(id);
	}

	private void cacheModel(Model model) {
		modelCache = new HashMap<>();
		modelCache.put(this, model);
	}

	public Model asRdf() {

		if (modelCache.containsKey(this)) {
			Model cachedModel = modelCache.get(this);
			if (!cachedModel.isEmpty()) {
				return cachedModel;
			}
		}

		ModelBuilder builder = new ModelBuilder();
		addTriples(builder);
		if (label != null) {
			builder.add(RDFS.LABEL, label);
		}
		Model model = builder.build();

		Model nestedModel = getReferencedResources().stream()
				.flatMap(resource -> resource.asRdf().stream())
				.collect(Collectors.toCollection(LinkedHashModel::new));

		model.addAll(nestedModel);

		cacheModel(model);

		return model;
	}



}
