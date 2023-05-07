import { createUriAndTermNamespace } from "@treecg/types";

export const RDF = createUriAndTermNamespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#',
  'type'
);

export const RML = createUriAndTermNamespace('http://semweb.mmlab.be/ns/rml#',
  'logicalSource', 'source', 'referenceFormulation', 'reference', 'iterator'
);

export const RMLS = createUriAndTermNamespace('http://semweb.mmlab.be/ns/rmls#',
  'hostName', 'port', "broker", "groupId", "topic", "KafkaStream"
);


export const RR = createUriAndTermNamespace("http://www.w3.org/ns/r2rml#",
  "TriplesMap", 'constant', 'termType', "IRI");

export const QL = createUriAndTermNamespace("http://semweb.mmlab.be/ns/ql#",
  'JSONPath', "CSV", "XPath")

export const CSVW = createUriAndTermNamespace("http://www.w3.org/ns/csvw#",
  'url', 'dialect', 'Dialect', 'delimiter', 'Table')
