# Notes on working with the RML specification

## Errors

* In the [http://rml.io/spec.html#example-JSON][json example] there are two small errors:
  * `@prefix wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#lat>.` should be `@prefix wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>.`, in both the mapping and example output,
  * in the input `" EU"` should be `"EU"`.
* In the note under [http://rml.io/spec.html#typed-literals][Typed literals], it says "O ne" instead of "One".
* In the vocabulary listing, under 'Classes', it should refer to `rml:ReferenceFormulation` instead of the property `rml:referenceFormulation` (both in text and link to the name space page).
* In the vocabulary listing, under 'Properties', the "Represents" value for `rr:subjectMap` links to [http://rml.io/spec.html#triples-map][Triples Maps] instead of [http://rml.io/spec.html#subject-map][Subject Maps].
* In the vocabulary listing, under 'Other Terms', the query languages are prefixed with `rml`; `rml:JSONPath`, `rml:XPath`. In the examples and the ontology, the prefix `ql` is used.
* In the vocabulary listing, under 'Other Terms', `ql:CSV` and `ql:CSS3` are missing. `ql:CSV` _is_ used in the examples.
* In the vocabulary listing, under 'Other Terms', the links for each value in the 'Denotes' column link to an anchor on the page that do not exist.

* What does this even mean?!
  > A logical source is any source that is mapped to RDF triples.

* `rml:baseSource` is defined as follows: 
  > A base source (rml:baseSource) is a logical source, rml:logicalSource, pointing to a source that contains the data to be mapped. 

  But it is not mentioned in the vocabulary listing, or on the namespace page

* In [http://rml.io/spec.html#logical-source][the section describing logical sources], it states:
  > The reference formulation must be specified in the case of databases and XML and JSON data sources. By default SQL2008 for databases, as SQL2008 is the default for R2RML, [XPath] for XML and JSONPath for JSON data sources.