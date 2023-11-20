# rml-mapper-processor-ts

[![Bun CI](https://github.com/julianrojas87/rml-mapper-processor-ts/actions/workflows/build-test.yml/badge.svg)](https://github.com/julianrojas87/rml-mapper-processor-ts/actions/workflows/build-test.yml) [![npm](https://img.shields.io/npm/v/rml-mapper-processor-ts.svg?style=popout)](https://npmjs.com/package/rml-mapper-processor-ts)

Typescript wrappers over the RML-related operations to be reused within the [Connector Architecture](hhttps://the-connector-architecture.github.io/site/docs/1_Home). Currently this repository exposes 3 functions:

### [`js:Y2R`](https://github.com/julianrojas87/rml-mapper-processor-ts/blob/main/processors.ttl#L9)

This processor takes a stream of YARRRML mapping files as input and converts them to their correspondent representation in RML quads. It relies on the [`yarrrml-parser`](https://github.com/RMLio/yarrrml-parser) library for executing the transformation.

### [`js:RMLMapperReader`](https://github.com/julianrojas87/rml-mapper-processor-ts/blob/main/processors.ttl#L44)

This processor is executes RML mapping rules using the Java-based [RMLMapper engine](https://github.com/RMLio/rmlmapper-java). A mapping process can be defined within a Connector Architecture (CA) pipeline, by defining an input stream of RML mappings, which will be executed sequentially. A set of logical sources (`js:rmlSource`) and targets (`js:rmlTarget`) can be optionally declared to make them visible to the CA pipeline. Otherwise a default output (`js:output`) needs to be defined, pointing to a file where all produced RDF triples/quads will be collected.

Logical sources can be marked as trigger-based (`js:trigger`) to indicate that they will be updated in the future and therefore, triggering new mapping executions. Finally, a path (`js:rmlJar`) to a local RMLMapper can be given. An example definition of the processor is shown next:

```turtle
@prefix : <https://w3id.org/conn#>.
@prefix js: <https://w3id.org/conn/js#>.

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

### [`js:IncRMLTransformer`](https://github.com/julianrojas87/rml-mapper-processor-ts/blob/main/processors.ttl#L142)

This processor transforms a given stream of RML mapping documents to their correspondent Incremental RML (IncRML) representation. Concretely, this means that every defined `rr:TriplesMap` (that has at least 1 defined `rr:predicateObjectMap`) is further expanded into 3 new `rr:TriplesMap`s, each one dedicated to handle entity `create`, `update` and `delete` events. This processor can be used within a CA pipeline as follows:

```turtle
@prefix : <https://w3id.org/conn#>.
@prefix js: <https://w3id.org/conn/js#>.
@prefix idlab-fn: <http://example.com/idlab/function/>.
@prefix as: <https://www.w3.org/ns/activitystreams#>.
@prefix dc: <http://purl.org/dc/terms/>.
@prefix formats: <http://www.w3.org/ns/formats/>.

[ ] a js:IncRMLTransformer; 
    js:rmlStream <rmlStream>;
    js:config [
        js:stateBasePath <./state>;
        js:lifeCycleConfig [
            js:predicate <http://ex.org/lifeCycleProp>;
            js:create [
                js:function idlab-fn:explicitCreate;
                js:type as:Create
            ];
            js:update [
                js:function idlab-fn:implicitUpdate;
                js:type as:Update
            ];
            js:delete [
                js:function idlab-fn:implicitDelete;
                js:type as:Delete
            ]
        ];
        js:targetConfig [ # LDES-based target configuration is optional
            js:targetPath <./output.nt>;
            js:timestampPath dc:modified;
            js:versionOfPath dc:isVersionOf;
            js:serialization formats:N-Triples;
            js:uniqueIRIs true;
            js:ldesBaseIRI <http://ex.org/my-ldes>;
            js:shape <http://ex.org/my-ldes/shape>
        ]
    ];
    js:incrmlStream <incrmlStream>;
    js:bulkMode true. # Optional, to handle multiple mapping files together
```

The configuration (`js:config`) of the processor includes a specification of the predicate (`js:lifeCycleConfig/js:predicate`) that will be used to characterize entities and the particular FnO functions that will be used to detect create, update and delete events.

Optionally, a LDES-based logical target configuration (`js:targetConfig`) can be given to produce unique IRIs for every entity and LDES-specific metadata.

Taking for example, the above processor configuration and the following RML mapping as input:

```turtle
<http://ex.org/m0> a rr:TriplesMap ;
    rml:logicalSource [
        a rml:LogicalSource ;
        rml:source "data.xml" ;
        rml:iterator "//Data" ;
        rml:referenceFormulation ql:XPath
    ] ;
    rr:subjectMap [
        a rr:SubjectMap ;
        rr:template "http://ex.org/instance/{prop1/@value}" ;
        rr:class "http://ex.org/ns/SomeClass" ;
    ] ;
    rr:predicateObjectMap [
        rr:predicate "http://ex.org/ns/someProperty" ;
        rr:objectMap [
            rml:reference "prop2/@value"
        ]
    ] .
```

The processor will expand the mapping to the following IncRML document:

```turtle
<http://ex.org/m0_create> a rr:TriplesMap ; # Create event
    rml:logicalSource _:bn0 ;
    rr:subjectMap [
        a rr:FunctionTermMap ;
        fnml:functionValue [
            rr:predicateObjectMap [
                rr:predicate fno:executes ;
                rr:objectMap [ rr:constant idlab-fn:explicitCreate ]
            ], [
                rr:predicate idlab-fn:iri ;
                rr:objectMap [ rr:template "http://ex.org/instance/{prop1/@value}" ]
            ], [
                rr:predicate idlab-fn:state ;
                rr:objectMap [ rr:constant "./state/3cd43073163c2153e4f6b01788350e0d_create_state" ]
            ]
        ] ;
        rml:logicalTarget _:bn1
    ] ;
    rr:predicateObjectMap _:bn2 ;
    rr:predicateObjectMap [
        rr:predicate <http://ex.org/lifeCycleProp> ;
        rr:objectMap [ rr:constant as:Create ]
    ] .

<http://ex.org/m0_update> a rr:TriplesMap ; # Update event
    rml:logicalSource _:bn0 ;
    rr:subjectMap [
        a rr:FunctionTermMap ;
        fnml:functionValue [
            rr:predicateObjectMap [
                rr:predicate fno:executes ;
                rr:objectMap [ rr:constant idlab-fn:implicitUpdate ]
            ], [
                rr:predicate idlab-fn:iri ;
                rr:objectMap [ rr:template "http://ex.org/instance/{prop1/@value}" ]
            ], [
                rr:predicate idlab-fn:watchedProperties ;
                rr:objectMap [ rr:template "prop0={prop1/@value}" ]
            ], [
                rr:predicate idlab-fn:state ;
                rr:objectMap [ rr:constant "./state/957ab073163c2153e4f6b01788323ab42_update_state" ]
            ]
        ] ;
        rml:logicalTarget _:bn1
    ] ;
    rr:predicateObjectMap _:bn2 ;
    rr:predicateObjectMap [
        rr:predicate <http://ex.org/lifeCycleProp> ;
        rr:objectMap [ rr:constant as:Update ]
    ] .

<http://ex.org/m0_delete> a rr:TriplesMap ; # Delete event
    rml:logicalSource _:bn0 ;
    rr:subjectMap [
        a rr:FunctionTermMap ;
        fnml:functionValue [
            rr:predicateObjectMap [
                rr:predicate fno:executes ;
                rr:objectMap [ rr:constant idlab-fn:implicitDelete ]
            ], [
                rr:predicate idlab-fn:iri ;
                rr:objectMap [ rr:template "http://ex.org/instance/{prop1/@value}" ]
            ], [
                rr:predicate idlab-fn:state ;
                rr:objectMap [ rr:constant "./state/67af43039445c2153e4f3920a788350fff3_delete_state" ]
            ]
        ] ;
        rml:logicalTarget _:bn1
    ] ;
    rr:predicateObjectMap [
        rr:predicate <http://ex.org/lifeCycleProp> ;
        rr:objectMap [ rr:constant as:Delete ]
    ] .

_:bn0 a rml:LogicalSource ;
    rml:source "data.xml" ;
    rml:iterator "//Data" ;
    rml:referenceFormulation ql:XPath .

_:bn1 a rmlt:EventStreamTarget ; # LDES-based Logical Target
    rmlt:target [
        a void:Dataset ;
        void:dataDump "./output.nt"
    ] ;
    rmlt:ldes [
        a ldes:EventStream ;
        ldes:timestampPath dc:modified ;
        ldes:versionOfPath dc:isVersionOf ;
        tree:shape <http://ex.org/my-ldes/shape>
    ] ;
    rmlt:serialization formats:N-Triples ;
    rmlt:ldesGenereateImmutableIRI true ;
    rmlt:ldesBaseIRI <http://ex.org/my-ldes> .

_:bn2 a rr:PredicateObjectMap ;
    rr:predicate "http://ex.org/ns/someProperty" ;
    rr:objectMap [
        rml:reference "prop2/@value"
    ] .
```
