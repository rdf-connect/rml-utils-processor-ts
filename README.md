# rml-utils-processor-ts

[![Build and tests with Node.js](https://github.com/rdf-connect/rml-utils-processor-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/rdf-connect/rml-utils-processor-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/@rdfc/rml-utils-processor-ts.svg?style=popout)](https://npmjs.com/package/@rdfc/rml-utils-processor-ts)

This repository provides RDF-Connect processors for working with RML (RDF Mapping Language) pipelines.
It includes utilities for converting YARRRML to RML, executing RML mappings through the Java RMLMapper, and transforming RML mappings into Incremental RML (IncRML).

These processors are designed to integrate with RDF-Connect pipelines using e.g., the [rdfc:NodeRunner](https://github.com/rdf-connect/js-runner).

## Usage

To use these processors, import the package into your RDF-Connect pipeline configuration and reference the required processors.

### Installation

Install dependencies and build the package locally:

```bash
npm install
npm run build
```

Or install from NPM:

```bash
npm install @rdfc/rml-utils-processor-ts
```

Next, add the processor definitions to your pipeline configuration:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.
```

The RML mapper processor executes a Java RMLMapper jar. If `rdfc:rmlJar` is omitted, the processor downloads the default RMLMapper release to `/tmp` and reuses it for the process lifetime.

## Processors and Configuration

### `rdfc:Yarrrml2RML` - YARRRML to RML Transformer

Transforms YARRRML mapping documents into RML mapping documents.

**Parameters:**

- `rdfc:input` (`rdfc:Reader`, required): Input channel that streams YARRRML mapping documents as strings.
- `rdfc:output` (`rdfc:Writer`, required): Output channel that receives generated RML mappings.

Example:

```turtle
<yarrrml2rml> a rdfc:Yarrrml2RML;
    rdfc:input <yarrrmlStream>;
    rdfc:output <rmlStream>.
```

---

### `rdfc:RMLMapperJS` - RML Mapper Processor

Executes RML mapping documents by wrapping the Java-based [RMLMapper](https://github.com/RMLio/rmlmapper-java) and streams the produced RDF to RDF-Connect channels.

The processor reads one or more RML mapping documents from `rdfc:mappings`. It can optionally receive RDF-Connect managed logical sources and logical targets. Declared logical sources are written to temporary files and matched against `rml:source` values in the mapping documents. Declared logical targets are matched against `void:dataDump` values in `rmlt:LogicalTarget` or `rmlt:EventStreamTarget` definitions.

**Parameters:**

- `rdfc:mappings` (`rdfc:Reader`, required): Input channel that streams RML mapping documents.
- `rdfc:output` (`rdfc:Writer`, required): Default output channel for RDF produced by mappings without an explicit logical target.
- `rdfc:rmlSource` (`rdfc:RmlSource`, optional): RDF-Connect managed logical source declarations.
- `rdfc:rmlTarget` (`rdfc:RmlTarget`, optional): RDF-Connect managed logical target declarations.
- `rdfc:rmlJar` (`string`, optional): Path or URL to an RMLMapper jar. If omitted, the default RMLMapper jar is downloaded automatically.

**`rdfc:RmlSource` parameters:**

- `rdfc:sourceLocation` (`string`, required): Source location as referenced by `rml:source` in the mapping document.
- `rdfc:input` (`rdfc:Reader`, required): Channel that provides source data.
- `rdfc:trigger` (`boolean`, optional): If true, this source can trigger additional mapping executions when new data arrives.
- `rdfc:incRMLStateIndex` (`string`, optional): Regex used to extract a source identifier from incoming data. The first capture group is appended to IncRML state paths so independent datasets coming via the same Logical Source can keep a separate state.

**`rdfc:RmlTarget` parameters:**

- `rdfc:targetLocation` (`string`, required): Target location as referenced by `void:dataDump` in the mapping document.
- `rdfc:output` (`rdfc:Writer`, required): Channel that receives RDF written to that logical target.

Example:

```turtle
<mapper> a rdfc:RMLMapperJS;
    rdfc:mappings <rmlStream>;
    rdfc:output <defaultOutput>;
    rdfc:rmlSource [
        rdfc:sourceLocation "dataset/data.xml";
        rdfc:input <dataInput>;
        rdfc:trigger true;
        rdfc:incRMLStateIndex "source_id=\"([^\"]+)\""
    ];
    rdfc:rmlTarget [
        rdfc:targetLocation "file:///results/output.nq";
        rdfc:output <targetOutput>
    ];
    rdfc:rmlJar "./rmlmapper-7.0.0-r374-all.jar".
```

---

### `rdfc:IncRMLTransformer` - Incremental RML Transformer

Transforms RML mapping documents into Incremental RML documents.

For every `rr:TriplesMap` that has at least one `rr:predicateObjectMap`, the processor creates IncRML mappings for entity create, update, and delete events. Triples maps that produce the same subject IRI, use the same logical source, and belong to the same named graph are merged into one event-specific triples map.

An optional LDES target configuration can be provided to generate LDES metadata and unique immutable IRIs.

**Parameters:**

- `rdfc:rmlStream` (`rdfc:Reader`, required): Input channel that streams RML mapping documents.
- `rdfc:config` (`rdfc:IncRMLConfig`, required): IncRML transformation configuration.
- `rdfc:incrmlStream` (`rdfc:Writer`, required): Output channel that receives IncRML mapping documents.

**`rdfc:IncRMLConfig` parameters:**

- `rdfc:stateBasePath` (`string`, required): Base path used for generated IncRML state files.
- `rdfc:lifeCycleConfig` (`rdfc:LifeCycleConfig`, required): Lifecycle predicate and function configuration.
- `rdfc:targetConfig` (`rdfc:LDESTargetConfig`, optional): LDES logical target configuration.

**`rdfc:LifeCycleConfig` parameters:**

- `rdfc:predicate` (`string`, required): Predicate used to describe the lifecycle event type.
- `rdfc:create` (`rdfc:EventConfig`, required): Function and RDF type for create events.
- `rdfc:update` (`rdfc:EventConfig`, required): Function and RDF type for update events.
- `rdfc:delete` (`rdfc:EventConfig`, required): Function and RDF type for delete events.

**`rdfc:EventConfig` parameters:**

- `rdfc:function` (`string`, required): FnO function IRI used for the event.
- `rdfc:type` (`string`, required): RDF type IRI emitted for the event.

**`rdfc:LDESTargetConfig` parameters:**

- `rdfc:targetPath` (`string`, required): Output path used in the generated logical target.
- `rdfc:timestampPath` (`string`, required): LDES timestamp path.
- `rdfc:versionOfPath` (`string`, required): LDES version-of path.
- `rdfc:serialization` (`string`, required): RML target serialization IRI.
- `rdfc:uniqueIRIs` (`boolean`, required): Whether generated LDES members should use immutable IRIs.
- `rdfc:ldesBaseIRI` (`string`, optional): Base IRI of the generated LDES.
- `rdfc:shape` (`string`, optional): TREE shape IRI for the generated LDES.

Example:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix idlab-fn: <https://w3id.org/imec/idlab/function#>.
@prefix as: <https://www.w3.org/ns/activitystreams#>.
@prefix dc: <http://purl.org/dc/terms/>.
@prefix formats: <http://www.w3.org/ns/formats/>.

<incrml> a rdfc:IncRMLTransformer;
    rdfc:rmlStream <rmlStream>;
    rdfc:config [
        rdfc:stateBasePath "./state";
        rdfc:lifeCycleConfig [
            rdfc:predicate "http://example.org/lifeCycleType";
            rdfc:create [
                rdfc:function idlab-fn:explicitCreate;
                rdfc:type as:Create
            ];
            rdfc:update [
                rdfc:function idlab-fn:implicitUpdate;
                rdfc:type as:Update
            ];
            rdfc:delete [
                rdfc:function idlab-fn:implicitDelete;
                rdfc:type as:Delete
            ]
        ];
        rdfc:targetConfig [
            rdfc:targetPath "./output.nt";
            rdfc:timestampPath dc:modified;
            rdfc:versionOfPath dc:isVersionOf;
            rdfc:serialization formats:N-Triples;
            rdfc:uniqueIRIs true;
            rdfc:ldesBaseIRI "http://example.org/my-ldes";
            rdfc:shape "http://example.org/my-ldes/shape"
        ]
    ];
    rdfc:incrmlStream <incrmlStream>.
```

## Example Pipelines

### Example 1: Convert YARRRML to RML

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.

<yarrrmlStream> a rdfc:Reader, rdfc:Writer.
<rmlStream> a rdfc:Reader, rdfc:Writer.

<yarrrml2rml> a rdfc:Yarrrml2RML;
    rdfc:input <yarrrmlStream>;
    rdfc:output <rmlStream>.
```

### Example 2: Run RML mappings over managed source data

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.

<rmlStream> a rdfc:Reader, rdfc:Writer.
<dataInput> a rdfc:Reader, rdfc:Writer.
<defaultOutput> a rdfc:Reader, rdfc:Writer.
<targetOutput> a rdfc:Reader, rdfc:Writer.

<mapper> a rdfc:RMLMapperJS;
    rdfc:mappings <rmlStream>;
    rdfc:output <defaultOutput>;
    rdfc:rmlSource [
        rdfc:sourceLocation "dataset/data.xml";
        rdfc:input <dataInput>
    ];
    rdfc:rmlTarget [
        rdfc:targetLocation "file:///results/output.nq";
        rdfc:output <targetOutput>
    ].
```

### Example 3: Convert RML to IncRML and execute it

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix idlab-fn: <https://w3id.org/imec/idlab/function#>.
@prefix as: <https://www.w3.org/ns/activitystreams#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.

<rmlStream> a rdfc:Reader, rdfc:Writer.
<incrmlStream> a rdfc:Reader, rdfc:Writer.
<dataInput> a rdfc:Reader, rdfc:Writer.
<output> a rdfc:Reader, rdfc:Writer.

<incrml> a rdfc:IncRMLTransformer;
    rdfc:rmlStream <rmlStream>;
    rdfc:config [
        rdfc:stateBasePath "./state";
        rdfc:lifeCycleConfig [
            rdfc:predicate "http://example.org/lifeCycleType";
            rdfc:create [ rdfc:function idlab-fn:explicitCreate; rdfc:type as:Create ];
            rdfc:update [ rdfc:function idlab-fn:implicitUpdate; rdfc:type as:Update ];
            rdfc:delete [ rdfc:function idlab-fn:implicitDelete; rdfc:type as:Delete ]
        ]
    ];
    rdfc:incrmlStream <incrmlStream>.

<mapper> a rdfc:RMLMapperJS;
    rdfc:mappings <incrmlStream>;
    rdfc:output <output>;
    rdfc:rmlSource [
        rdfc:sourceLocation "dataset/data.xml";
        rdfc:input <dataInput>;
        rdfc:trigger true
    ].
```
