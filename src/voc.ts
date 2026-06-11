import { createUriAndTermNamespace } from "@treecg/types";
import type { NamedNode } from "@rdfjs/types";

export * from "@treecg/types";

type TermVocabulary<T extends string> = {
  namespace: NamedNode<string>;
  custom: (input: string) => NamedNode<string>;
} & {
  [K in T]: NamedNode<string>;
};

type Vocabulary<T extends string> = {
  namespace: string;
  custom: (input: string) => string;
  terms: TermVocabulary<T>;
} & {
  [K in T]: string;
};

function createVocabulary<T extends string>(
  baseUri: string,
  ...localNames: T[]
): Vocabulary<T> {
  return createUriAndTermNamespace(baseUri, ...localNames) as unknown as Vocabulary<T>;
}

export const VOID = createVocabulary(
  "http://rdfs.org/ns/void#",
  "Dataset",
  "dataDump",
);
export const RDF = createVocabulary(
  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "type",
);

export const RML = createVocabulary(
  "http://semweb.mmlab.be/ns/rml#",
  "LogicalSource",
  "logicalSource",
  "logicalTarget",
  "source",
  "referenceFormulation",
  "reference",
  "iterator",
);

export const RMLS = createVocabulary(
  "http://semweb.mmlab.be/ns/rmls#",
  "hostName",
  "port",
  "broker",
  "groupId",
  "topic",
  "KafkaStream",
);

export const RMLT = createVocabulary(
  "http://semweb.mmlab.be/ns/rml-target#",
  "LogicalTarget",
  "EventStreamTarget",
  "ldes",
  "ldesBaseIRI",
  "serialization",
  "target",
  "ldesGenerateImmutableIRI"
);

export const RR = createVocabulary(
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

export const FNML = createVocabulary(
  "http://semweb.mmlab.be/ns/fnml#",
  "FunctionTermMap",
  "functionValue",
);

export const FNO = createVocabulary(
  "https://w3id.org/function/ontology#",
  "executes"
);

export const QL = createVocabulary(
  "http://semweb.mmlab.be/ns/ql#",
  "JSONPath",
  "CSV",
  "XPath",
);

export const CSVW = createVocabulary(
  "http://www.w3.org/ns/csvw#",
  "url",
  "dialect",
  "Dialect",
  "delimiter",
  "Table",
);

export const GREL = createVocabulary(
  "http://users.ugent.be/~bjdmeest/function/grel.ttl#",
  "array_join",
  "param_a",
  "param_string_sep",
  "valueParameter",
  "valueParameter2"
);

export const IDLAB_FN = createVocabulary(
  "https://w3id.org/imec/idlab/function#",
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

export const AS = createVocabulary(
  "https://www.w3.org/ns/activitystreams#",
  "Create",
  "Update",
  "Delete"
);
