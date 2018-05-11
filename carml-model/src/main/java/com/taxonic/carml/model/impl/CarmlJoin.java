package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Join;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.vocab.Rr;
import java.util.Objects;

public class CarmlJoin implements Join{

	private String child;
	private String parent;

	public CarmlJoin() {}

	public CarmlJoin(
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
		return "CarmlJoin [getChildReference()=" + getChildReference() + ", getParentReference()=" + getParentReference() + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(child, parent);
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
		CarmlJoin other = (CarmlJoin) obj;
		return Objects.equals(child, other.child) && Objects.equals(parent, other.parent);
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

		public CarmlJoin build() {
			return new CarmlJoin(
				child,
				parent
			);
		}
	}
}
