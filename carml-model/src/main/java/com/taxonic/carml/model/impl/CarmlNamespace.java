package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Namespace;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Carml;

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
		return "CarmlNamespace [prefix=" + prefix + ", name=" + name + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CarmlNamespace other = (CarmlNamespace) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (prefix == null) {
			if (other.prefix != null)
				return false;
		} else if (!prefix.equals(other.prefix))
			return false;
		return true;
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
