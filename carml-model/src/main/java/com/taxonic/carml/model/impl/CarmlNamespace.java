package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Namespace;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;
import java.util.Objects;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class CarmlNamespace implements Namespace {

	private String prefix;
	private String name;

	public CarmlNamespace() {}

	public CarmlNamespace(String prefix, String name) {
		this.prefix = prefix;
		this.name = name;
	}

	@RdfProperty(Carml.namespacePrefix)
	@Override
	public String getPrefix() {
		return prefix;
	}

	@RdfProperty(Carml.namespaceName)
	@Override
	public String getName() {
		return name;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(prefix, name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CarmlNamespace other = (CarmlNamespace) obj;
		return Objects.equals(prefix, other.prefix) && Objects.equals(name, other.name);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder{

		private String prefix;
		private String name;

		Builder() {}

		public Builder prefix(String prefix) {
			this.prefix = prefix;
			return this;
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public CarmlNamespace build() {
			return new CarmlNamespace(prefix, name);
		}
	}

}
