import { createUriAndTermNamespace } from "@treecg/types";

export * from "@treecg/types";

export const VOID = createUriAndTermNamespace(
  "http://rdfs.org/ns/void#",
  "dataDump",
);
export const RDF = createUriAndTermNamespace(
  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "type",
);

export const RML = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/rml#",
  "logicalSource",
  "source",
  "referenceFormulation",
  "reference",
  "iterator",
  "LogicalSource"
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
  "serialization",
  "target",
);

export const RR = createUriAndTermNamespace(
  "http://www.w3.org/ns/r2rml#",
  "TriplesMap",
  "SubjectMap",
  "PredicateObjectMap",
  "GraphMap",
  "constant",
  "termType",
  "IRI",
);

export const FNML = createUriAndTermNamespace(
  "http://semweb.mmlab.be/ns/fnml#",
  "FunctionTermMap",
  "functionValue",
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
