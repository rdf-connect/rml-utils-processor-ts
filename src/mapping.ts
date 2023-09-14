import { Store, DataFactory } from "n3";
import { BlankNode, Literal, NamedNode } from "@rdfjs/types";
import { RDF } from "@treecg/types";
import { CSVW, QL, RML, RMLS, RR } from "./voc";
import { FileReaderConfig, KafkaReaderConfig, ReaderConfig } from "@treecg/connector-all";
import { recursiveDeleteQuad } from "./util";

export type SourceConfig = {
  referenceFormulation?: NamedNode,
  iterator?: Literal,
};

function setFileSource(logicalSource: NamedNode | BlankNode, config: SourceConfig, readerConfig: FileReaderConfig, store: Store) {
  const file = readerConfig.path;
  if (file.endsWith("json") || file.endsWith("xml") || config.referenceFormulation?.value == QL.XPath || config.referenceFormulation?.value === QL.JSONPath) {
    const ref = config.referenceFormulation || (file.endsWith("json") ? QL.terms.JSONPath : QL.terms.XPath);
    const iterator = config.iterator || (file.endsWith("json") ? DataFactory.literal("$") : DataFactory.literal("/"));
    store.addQuads([
      DataFactory.quad(logicalSource, RML.terms.source, DataFactory.literal(file)),
      DataFactory.quad(logicalSource, RML.terms.referenceFormulation, ref),
      DataFactory.quad(logicalSource, RML.terms.iterator, iterator),
    ]);
  } else if (file.endsWith("tsv") || file.endsWith("csv")) {
    const id = store.createBlankNode();
    const dialect = store.createBlankNode();
    const extras = file.endsWith("tsv") ? [
      DataFactory.quad(id, CSVW.terms.dialect, dialect),
      DataFactory.quad(dialect, RDF.terms.type, CSVW.terms.Dialect),
      DataFactory.quad(dialect, CSVW.terms.delimiter, DataFactory.literal("\\t")),
    ] : [];

    store.addQuads([
      DataFactory.quad(logicalSource, RML.terms.source, id),
      DataFactory.quad(id, RDF.terms.type, CSVW.terms.Table),
      DataFactory.quad(id, CSVW.terms.url, DataFactory.literal(file)),
      DataFactory.quad(logicalSource, RML.terms.referenceFormulation, QL.terms.CSV),
      ...extras,
    ]);
  } else {
    throw "Could not deduce enough information from filename " + file;
  }
}
function setKafkaSource(logicalSource: NamedNode | BlankNode, config: SourceConfig, readerConfig: KafkaReaderConfig, store: Store) {
  const ref = config.referenceFormulation || QL.terms.JSONPath;
  const iterator = config.iterator || DataFactory.literal("$");
  const id = store.createBlankNode();
  store.addQuads([
    DataFactory.quad(logicalSource, RML.terms.referenceFormulation, ref),
    DataFactory.quad(logicalSource, RML.terms.iterator, iterator),
    DataFactory.quad(logicalSource, RML.terms.source, id),
    DataFactory.quad(id, RDF.terms.type, RMLS.terms.KafkaStream),
    DataFactory.quad(id, RMLS.terms.topic, DataFactory.literal(readerConfig.topic.name)),
    DataFactory.quad(id, RMLS.terms.broker, DataFactory.literal(<string>readerConfig.broker)),
    DataFactory.quad(id, RMLS.terms.groupId, DataFactory.literal(readerConfig.consumer.groupId)),
  ]);
}
export function handleLogicalSource(store: Store, config: { "type": string; config: ReaderConfig; }, rml: SourceConfig) {
  const mappings = store.getSubjects(RDF.terms.type, RR.terms.TriplesMap, null);
  if (mappings.length !== 1) {
    throw `Expected one mapping, found ${mappings.length}`;
  }
  const mapping = mappings[0];

  // Delete lingering logicalSources
  store.getQuads(mapping, RML.terms.logicalSource, null, null)
    .forEach(s => recursiveDeleteQuad(s, store));

  // Delete lingering logicalTargets
  //
  store.getQuads(mapping, RML.custom("logicalTarget"), null, null)
    .forEach(s => recursiveDeleteQuad(s, store));

  const id = store.createBlankNode();
  const logicalSource = id;
  store.addQuads([
    DataFactory.quad(
      mapping, RML.terms.logicalSource, id
    ),
  ]);

  switch (config.type) {
    case "file":
      const fileConfig = <FileReaderConfig>config.config;
      setFileSource(<NamedNode | BlankNode>logicalSource, rml, fileConfig, store);
      break;
    case "kafka":
      const kafkaConfig = <KafkaReaderConfig>config.config;
      setKafkaSource(<NamedNode | BlankNode>logicalSource, rml, kafkaConfig, store);
      break;
    default:
      throw `RML Streamer does not support ${config.type} as source`;

  }
}

