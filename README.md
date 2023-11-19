# rml-mapper-processor-ts

[![Bun CI](https://github.com/julianrojas87/rml-mapper-processor-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/julianrojas87/rml-mapper-processor-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/rml-mapper-processor-ts.svg?style=popout)](https://npmjs.com/package/rml-mapper-processor-ts)

Typescript wrappers over the RML-related operations to be reused within the [Connector Architecture](hhttps://the-connector-architecture.github.io/site/docs/1_Home). Currently this repository exposes 2 functions:

### [`js:Y2R`](https://github.com/julianrojas87/rml-mapper-processor-ts/blob/main/processors.ttl#L9)

This processor takes a stream of YARRRML mapping files as input and converts them to their correspondent representation in RML quads. It relies on the [`yarrrml-parser`](https://github.com/RMLio/yarrrml-parser) library for executing the transformation.

### [`js:RMLMapperReader`](https://github.com/julianrojas87/rml-mapper-processor-ts/blob/main/processors.ttl#L44)

This processor is executes RML mapping rules using the Java-based [RMLMapper engine](https://github.com/RMLio/rmlmapper-java). A mapping process can be defined within a Connector Architecture (CA) pipeline, by defining an input stream of RML mappings, which will be executed sequentially. A set of logical sources (`js:rmlSource`) and targets (`js:rmlTarget`) can be optionally declared to make them visible to the CA pipeline. Otherwise a default output (`js:output`) needs to be defined, pointing to a file where all produced RDF triples/quads will be collected.

Logical sources can be marked as trigger-based (`js:trigger`) to indicate that they will be updated in the future and therefore, triggering new mapping executions. Finally, a path (`js:rmlJar`) to a local RMLMapper can be given. An example definition of the processor is shown next:

```turtle
[ ] a js:RMLMapperReader; 
    js:rmlSource [
        js:sourceLocation "dataset/data.xml";
        js:input <fileChannelReader1>;
        js:trigger true
    ], [
        js:sourceLocation "dataset/static_data.json";
        js:input <fileChannelReader2>;
    ];
    js:rmlTarget [
        js:targetLocation "dataset/output.nt";
        js:output <fileChannelWriter>
    ];
    js:mappings <jr>;
    # js:output <jw>; # This parameter is only needed if no js:rmlTarget are defined
    js:rmlJar <./rmlmapper-6.3.0-r0-all.jar>.
```
