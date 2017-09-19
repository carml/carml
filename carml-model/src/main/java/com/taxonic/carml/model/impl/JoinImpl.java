package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Join;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rr;

public class JoinImpl implements Join{

	private String child;
	private String parent;
	
	public JoinImpl() {}
	
	public JoinImpl(
			String child,
			String parent
	) {
		this.child = child;
		this.parent = parent;
	}
	
	@RdfProperty(Rr.child)
	@Override
	public String getChildReference() {
		return child;
	}
	
	public void setChildReference(String child) {
		this.child = child;
	}
	
	@RdfProperty(Rr.parent)
	@Override
	public String getParentReference() {
		return parent;
	}
	
	public void setParentReference(String parent) {
		this.parent = parent;
	}
	
	@Override
	public String toString() {
		return "JoinImpl [getChildReference()=" + getChildReference() + ", getParentReference()=" + getParentReference() + "]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((child == null) ? 0 : child.hashCode());
		result = prime * result + ((parent == null) ? 0 : parent.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		JoinImpl other = (JoinImpl) obj;
		if (child == null) {
			if (other.child != null) return false;
		}
		else if (!child.equals(other.child)) return false;
		if (parent == null) {
			if (other.parent != null) return false;
		}
		else if (!parent.equals(other.parent)) return false;
		return true;
	}

	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder{
		
		private String child;
		private String parent;
		
		Builder() {}
		
		public Builder child(String child) {
			this.child = child;
			return this;
		}
		
		public Builder parent(String parent) {
			this.parent = parent;
			return this;
		}
		
		public JoinImpl build() {
			return new JoinImpl(
				child,
				parent
			);
		}
	}
}
