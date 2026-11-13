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
@prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.
```

The RML mapper processor executes a Java RMLMapper jar. If `rdfcp:rmlJar` is omitted, the processor downloads the default RMLMapper release to `/tmp` and reuses it for the process lifetime.

## Processors and Configuration

### `rdfcp:Yarrrml2RML` - YARRRML to RML Transformer

Transforms YARRRML mapping documents into RML mapping documents.

**Parameters:**

- `rdfcp:input` (`rdfc:Reader`, required): Input channel that streams YARRRML mapping documents as strings.
- `rdfcp:output` (`rdfc:Writer`, required): Output channel that receives generated RML mappings.

Example:

```turtle
<yarrrml2rml> a rdfcp:Yarrrml2RML;
    rdfcp:input <yarrrmlStream>;
    rdfcp:output <rmlStream>.
```

---

### `rdfcp:RMLMapperJS` - RML Mapper Processor

Executes RML mapping documents by wrapping the Java-based [RMLMapper](https://github.com/RMLio/rmlmapper-java) and streams the produced RDF to RDF-Connect channels.

The processor reads one or more RML mapping documents from `rdfcp:input`. It can optionally receive RDF-Connect managed logical sources and logical targets. Declared logical sources are written to temporary files and matched against `rml:source` values in the mapping documents. Declared logical targets are matched against `void:dataDump` values in `rmlt:LogicalTarget` or `rmlt:EventStreamTarget` definitions.

**Parameters:**

- `rdfcp:input` (`rdfc:Reader`, required): Input channel that streams RML mapping documents.
- `rdfcp:output` (`rdfc:Writer`, required): Default output channel for RDF produced by mappings without an explicit logical target.
- `rdfcp:rmlSource` (`rdfcp:RmlSource`, optional): RDF-Connect managed logical source declarations.
- `rdfcp:rmlTarget` (`rdfcp:RmlTarget`, optional): RDF-Connect managed logical target declarations.
- `rdfcp:rmlJar` (`string` or IRI, optional): Path or URL to an RMLMapper jar. If omitted, the default RMLMapper jar is downloaded automatically.

**`rdfcp:RmlSource` parameters:**

- `rdfcp:sourceLocation` (`string`, required): Source location as referenced by `rml:source` in the mapping document.
- `rdfcp:input` (`rdfc:Reader`, required): Channel that provides source data.
- `rdfcp:trigger` (`boolean`, optional): If true, this source can trigger additional mapping executions when new data arrives. Default: `false`.
- `rdfcp:incRMLStateIndex` (`string`, optional): Regex used to extract a source identifier from incoming data. The first capture group is appended to IncRML state paths so independent datasets coming via the same Logical Source can keep a separate state.

**`rdfcp:RmlTarget` parameters:**

- `rdfcp:targetLocation` (`string` or IRI, required): Target location as referenced by `void:dataDump` in the mapping document. It is compared as is, so an IRI must be absolute, like `<file:///results/output.nq>`: a relative one is resolved against the pipeline and no longer matches.
- `rdfcp:output` (`rdfc:Writer`, required): Channel that receives RDF written to that logical target.

Example:

```turtle
<mapper> a rdfcp:RMLMapperJS;
    rdfcp:input <rmlStream>;
    rdfcp:output <defaultOutput>;
    rdfcp:rmlSource [
        rdfcp:sourceLocation "dataset/data.xml";
        rdfcp:input <dataInput>;
        rdfcp:trigger true;
        rdfcp:incRMLStateIndex "source_id=\"([^\"]+)\""
    ];
    rdfcp:rmlTarget [
        rdfcp:targetLocation "file:///results/output.nq";
        rdfcp:output <targetOutput>
    ];
    rdfcp:rmlJar "./rmlmapper-7.0.0-r374-all.jar".
```

---

### `rdfcp:IncRMLTransformer` - Incremental RML Transformer

Transforms RML mapping documents into Incremental RML documents.

For every `rr:TriplesMap` that has at least one `rr:predicateObjectMap`, the processor creates IncRML mappings for entity create, update, and delete events. Triples maps that produce the same subject IRI, use the same logical source, and belong to the same named graph are merged into one event-specific triples map.

An optional LDES target configuration can be provided to generate LDES metadata and unique immutable IRIs.

**Parameters:**

- `rdfcp:input` (`rdfc:Reader`, required): Input channel that streams RML mapping documents.
- `rdfcp:config` (`rdfcp:IncRMLConfig`, required): IncRML transformation configuration.
- `rdfcp:output` (`rdfc:Writer`, required): Output channel that receives IncRML mapping documents.

**`rdfcp:IncRMLConfig` parameters:**

- `rdfcp:stateBasePath` (`string` or IRI, required): Base path used for generated IncRML state files.
- `rdfcp:lifeCycleConfig` (`rdfcp:LifeCycleConfig`, required): Lifecycle predicate and function configuration.
- `rdfcp:targetConfig` (`rdfcp:LDESTargetConfig`, optional): LDES logical target configuration.

**`rdfcp:LifeCycleConfig` parameters:**

- `rdfcp:predicate` (`string` or IRI, required): Predicate used to describe the lifecycle event type.
- `rdfcp:create` (`rdfcp:EventConfig`, required): Function and RDF type for create events.
- `rdfcp:update` (`rdfcp:EventConfig`, required): Function and RDF type for update events.
- `rdfcp:delete` (`rdfcp:EventConfig`, required): Function and RDF type for delete events.

**`rdfcp:EventConfig` parameters:**

- `rdfcp:function` (`string` or IRI, required): FnO function IRI used for the event.
- `rdfcp:type` (`string` or IRI, required): RDF type IRI emitted for the event.

**`rdfcp:LDESTargetConfig` parameters:**

- `rdfcp:targetPath` (`string`, required): Output path used in the generated logical target.
- `rdfcp:timestampPath` (`string` or IRI, required): LDES timestamp path.
- `rdfcp:versionOfPath` (`string` or IRI, required): LDES version-of path.
- `rdfcp:serialization` (`string` or IRI, required): RML target serialization IRI.
- `rdfcp:uniqueIRIs` (`boolean`, required): Whether generated LDES members should use immutable IRIs.
- `rdfcp:ldesBaseIRI` (`string` or IRI, optional): Base IRI of the generated LDES.
- `rdfcp:shape` (`string` or IRI, optional): TREE shape IRI for the generated LDES.

Example:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
@prefix idlab-fn: <https://w3id.org/imec/idlab/function#>.
@prefix as: <https://www.w3.org/ns/activitystreams#>.
@prefix dc: <http://purl.org/dc/terms/>.
@prefix formats: <http://www.w3.org/ns/formats/>.

<incrml> a rdfcp:IncRMLTransformer;
    rdfcp:input <rmlStream>;
    rdfcp:config [
        rdfcp:stateBasePath "./state";
        rdfcp:lifeCycleConfig [
            rdfcp:predicate "http://example.org/lifeCycleType";
            rdfcp:create [
                rdfcp:function idlab-fn:explicitCreate;
                rdfcp:type as:Create
            ];
            rdfcp:update [
                rdfcp:function idlab-fn:implicitUpdate;
                rdfcp:type as:Update
            ];
            rdfcp:delete [
                rdfcp:function idlab-fn:implicitDelete;
                rdfcp:type as:Delete
            ]
        ];
        rdfcp:targetConfig [
            rdfcp:targetPath "./output.nt";
            rdfcp:timestampPath dc:modified;
            rdfcp:versionOfPath dc:isVersionOf;
            rdfcp:serialization formats:N-Triples;
            rdfcp:uniqueIRIs true;
            rdfcp:ldesBaseIRI "http://example.org/my-ldes";
            rdfcp:shape "http://example.org/my-ldes/shape"
        ]
    ];
    rdfcp:output <incrmlStream>.
```

## Example Pipelines

### Example 1: Convert YARRRML to RML

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.

<yarrrmlStream> a rdfc:Reader, rdfc:Writer.
<rmlStream> a rdfc:Reader, rdfc:Writer.

<yarrrml2rml> a rdfcp:Yarrrml2RML;
    rdfcp:input <yarrrmlStream>;
    rdfcp:output <rmlStream>.
```

### Example 2: Run RML mappings over managed source data

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.

<rmlStream> a rdfc:Reader, rdfc:Writer.
<dataInput> a rdfc:Reader, rdfc:Writer.
<defaultOutput> a rdfc:Reader, rdfc:Writer.
<targetOutput> a rdfc:Reader, rdfc:Writer.

<mapper> a rdfcp:RMLMapperJS;
    rdfcp:input <rmlStream>;
    rdfcp:output <defaultOutput>;
    rdfcp:rmlSource [
        rdfcp:sourceLocation "dataset/data.xml";
        rdfcp:input <dataInput>
    ];
    rdfcp:rmlTarget [
        rdfcp:targetLocation "file:///results/output.nq";
        rdfcp:output <targetOutput>
    ].
```

### Example 3: Convert RML to IncRML and execute it

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix rdfcp: <https://w3id.org/rdf-connect/processors#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix idlab-fn: <https://w3id.org/imec/idlab/function#>.
@prefix as: <https://www.w3.org/ns/activitystreams#>.

<> owl:imports <./node_modules/@rdfc/rml-utils-processor-ts/processors.ttl>.

<rmlStream> a rdfc:Reader, rdfc:Writer.
<incrmlStream> a rdfc:Reader, rdfc:Writer.
<dataInput> a rdfc:Reader, rdfc:Writer.
<output> a rdfc:Reader, rdfc:Writer.

<incrml> a rdfcp:IncRMLTransformer;
    rdfcp:input <rmlStream>;
    rdfcp:config [
        rdfcp:stateBasePath "./state";
        rdfcp:lifeCycleConfig [
            rdfcp:predicate "http://example.org/lifeCycleType";
            rdfcp:create [ rdfcp:function idlab-fn:explicitCreate; rdfcp:type as:Create ];
            rdfcp:update [ rdfcp:function idlab-fn:implicitUpdate; rdfcp:type as:Update ];
            rdfcp:delete [ rdfcp:function idlab-fn:implicitDelete; rdfcp:type as:Delete ]
        ]
    ];
    rdfcp:output <incrmlStream>.

<mapper> a rdfcp:RMLMapperJS;
    rdfcp:input <incrmlStream>;
    rdfcp:output <output>;
    rdfcp:rmlSource [
        rdfcp:sourceLocation "dataset/data.xml";
        rdfcp:input <dataInput>;
        rdfcp:trigger true
    ].
```
