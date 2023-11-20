import { createUriAndTermNamespace } from "@treecg/types";

export * from "@treecg/types";

export const VOID = createUriAndTermNamespace(
  "http://rdfs.org/ns/void#",
  "Dataset",
  "dataDump",
);
export const RDF = createUriAndTermNamespace(
  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "type",
);

export const RML = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/rml#",
  "LogicalSource",
  "logicalSource",
  "logicalTarget",
  "source",
  "referenceFormulation",
  "reference",
  "iterator",
);

export const RMLS = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/rmls#",
  "hostName",
  "port",
  "broker",
  "groupId",
  "topic",
  "KafkaStream",
);

export const RMLT = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/rml-target#",
  "LogicalTarget",
  "EventStreamTarget",
  "ldes",
  "ldesBaseIRI",
  "serialization",
  "target",
  "ldesGenerateImmutableIRI"
);

export const RR = createUriAndTermNamespace(
  "http://www.w3.org/ns/r2rml#",
  "FunctionTermMap",
  "TriplesMap",
  "SubjectMap",
  "PredicateObjectMap",
  "GraphMap",
  "class",
  "constant",
  "dataType",
  "objectMap",
  "predicate",
  "predicateObjectMap",
  "predicateMap",
  "subjectMap",
  "graphMap",
  "template",
  "termType",
  "IRI",
);

export const FNML = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/fnml#",
  "FunctionTermMap",
  "functionValue",
);

export const FNO = createUriAndTermNamespace(
  "https://w3id.org/function/ontology#",
  "executes"
);

export const QL = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/ql#",
  "JSONPath",
  "CSV",
  "XPath",
);

export const CSVW = createUriAndTermNamespace(
  "http://www.w3.org/ns/csvw#",
  "url",
  "dialect",
  "Dialect",
  "delimiter",
  "Table",
);

export const GREL = createUriAndTermNamespace(
  "http://users.ugent.be/~bjdmeest/function/grel.ttl#",
  "array_join",
  "param_a",
  "param_string_sep",
  "valueParameter",
  "valueParameter2"
);

export const IDLAB_FN = createUriAndTermNamespace(
  "http://example.com/idlab/function/",
  "iri",
  "explicitCreate",
  "implicitUpdate",
  "implicitDelete",
  "trueCondition",
  "strBoolean",
  "state",
  "str",
  "watchedProperty"
);

export const AS = createUriAndTermNamespace(
  "https://www.w3.org/ns/activitystreams#",
  "Create",
  "Update",
  "Delete"
);
