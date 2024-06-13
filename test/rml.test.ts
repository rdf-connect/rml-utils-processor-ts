import { describe, test, expect, afterAll } from "@jest/globals";
import { SimpleStream } from "@rdfc/js-runner";
import { Parser, Store } from "n3";
import { deleteAsync } from "del";
import { rmlMapper, Source, Target } from "../src/rml/rml";
import { AS, RDF, RDFS } from "../src/voc";

describe("Functional tests for the rmlMapper Connector Architecture function", () => {
    const PREFIXES = `
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix rml: <http://semweb.mmlab.be/ns/rml#> .
        @prefix rmlt: <http://semweb.mmlab.be/ns/rml-target#> .
        @prefix fno: <https://w3id.org/function/ontology#> .
        @prefix fnml: <http://semweb.mmlab.be/ns/fnml#> .
        @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
        @prefix idlab-fn: <https://w3id.org/imec/idlab/function#> .
        @prefix void: <http://rdfs.org/ns/void#> .
        @prefix formats: <http://www.w3.org/ns/formats/> .
        @prefix td: <https://www.w3.org/2019/wot/td#> .
        @prefix htv: <http://www.w3.org/2011/http#> .
        @prefix hctl: <https://www.w3.org/2019/wot/hypermedia#> .
        @prefix ldes: <https://w3id.org/ldes#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix ex: <http://example.org/> .
        @prefix as: <https://www.w3.org/ns/activitystreams#> .
    `;

    const RML_TM_LOCAL_SOURCE_AND_TARGET = (source?: string) => {
        return `
        ex:map_test-mapping_000 a rr:TriplesMap ;
            rdfs:label "test-mapping" ;
            rml:logicalSource [
                a rml:LogicalSource ;
                rml:source "${source || "dataset/data.xml"}" ;
                rml:iterator "//data" ;
                rml:referenceFormulation ql:XPath
            ] ;
            rr:subjectMap [
                a rr:SubjectMap ;
                rr:template "http://example.org/{@id}" ;
                rml:logicalTarget [
                    a rmlt:LogicalTarget ;
                    rmlt:serialization formats:N-Quads ;
                    rmlt:target [
                        a void:Dataset ;
                        void:dataDump <file:///results/output.nq>
                    ]
                ] ;
                rr:graphMap [
                    a rr:GraphMap ;
                    rr:constant "http://example.org/myNamedGraph"
                ]
            ] ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:constant <http://example.org/Entity> ;
                    rr:termType rr:IRI
                ]
            ] ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant rdfs:label
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rml:reference "@label" ;
                    rr:termType rr:Literal
                ]
            ] .
        `;
    }

    const RML_TM_LOCAL_SOURCE_AND_LDES_TARGET = `
        ex:map_test-mapping_000 a rr:TriplesMap ;
            rdfs:label "test-mapping" ;
            rml:logicalSource [
                a rml:LogicalSource ;
                rml:source "dataset/data.xml" ;
                rml:iterator "//data" ;
                rml:referenceFormulation ql:XPath
            ] ;
            rr:subjectMap [
                a rr:SubjectMap ;
                rr:template "http://example.org/{@id}" ;
                rml:logicalTarget [
                    a rmlt:EventStreamTarget ;
                    rmlt:serialization formats:N-Quads ;
                    rmlt:target [
                        a void:Dataset ;
                        void:dataDump <file:///results/output.nq>
                    ];
                    rmlt:ldes [ 
                        a ldes:EvenStream;
                        ldes:timestampPath dct:modified;
                        ldes:versionOfPath dct:isVersionOf
                    ];
                    rmlt:ldesGenerateImmutableIRI "true"^^xsd:boolean
                ] ;
                rr:graphMap [
                    a rr:GraphMap ;
                    rr:constant "http://example.org/myNamedGraph"
                ]
            ] ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:constant <http://example.org/Entity> ;
                    rr:termType rr:IRI
                ]
            ] ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant rdfs:label
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rml:reference "@label" ;
                    rr:termType rr:Literal
                ]
            ] .
    `;

    const RML_TM_LOCAL_SOURCE_AND_NO_TARGET = (source?) => {
        return `
        ex:map_test-mapping_000 a rr:TriplesMap ;
            rdfs:label "test-mapping" ;
            rml:logicalSource [
                a rml:LogicalSource ;
                rml:source "${source || "dataset/data.xml"}" ;
                rml:iterator "//data" ;
                rml:referenceFormulation ql:XPath
            ] ;
            rr:subjectMap [
                a rr:SubjectMap ;
                rr:template "http://example.org/{@id}" ;
                rr:graphMap [
                    a rr:GraphMap ;
                    rr:constant "http://example.org/myNamedGraph"
                ]
            ] ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:constant <http://example.org/Entity> ;
                    rr:termType rr:IRI
                ]
            ] ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant rdfs:label
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rml:reference "@label" ;
                    rr:termType rr:Literal
                ]
            ] .
        `;
    };

    const RML_TM_STATEFUL = `
        ex:logical_source a rml:LogicalSource ;
            rml:source "dataset/data.xml" ;
            rml:iterator "//data" ;
            rml:referenceFormulation ql:XPath .

        ex:map_test-mapping_000 a rr:TriplesMap ;
            rdfs:label "test-mapping-create" ;
            rml:logicalSource ex:logical_source ;
            rr:subjectMap [
                fnml:functionValue [
                    rr:predicateObjectMap [
                        rr:predicate fno:executes ;
                        rr:objectMap [ rr:constant idlab-fn:explicitCreate ]
                    ] ;
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:iri ;
                        rr:objectMap [ rr:template "http://example.org/{@id}" ]
                    ];
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:state ;
                        rr:objectMap [ rr:constant "/tmp/create_state"; rr:dataType xsd:string; ]
                    ];
                ];
                rr:class <http://example.org/Entity>;
            ];
            rr:predicateObjectMap ex:pom_001 ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicate ex:lifeCycleType ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:constant as:Create ;
                    rr:termType rr:IRI
                ]
            ] .

            ex:map_test-mapping_001 a rr:TriplesMap ;
            rdfs:label "test-mapping-update" ;
            rml:logicalSource ex:logical_source ;
            rr:subjectMap [
                fnml:functionValue [
                    rr:predicateObjectMap [
                        rr:predicate fno:executes ;
                        rr:objectMap [ rr:constant idlab-fn:implicitUpdate ]
                    ] ;
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:iri ;
                        rr:objectMap [ rr:template "http://example.org/{@id}" ]
                    ];
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:watchedProperty ;
                        rr:objectMap [ rml:reference "('prop0=' || @id || 'prop1=' || @label)" ]
                    ];
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:state ;
                        rr:objectMap [ rr:constant "/tmp/update_state"; rr:dataType xsd:string; ]
                    ];
                ];
                rr:class <http://example.org/Entity>;
            ];
            rr:predicateObjectMap ex:pom_001 ;
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicate ex:lifeCycleType ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:constant as:Update ;
                    rr:termType rr:IRI
                ]
            ] .
        
        ex:map_test-mapping_002 a rr:TriplesMap ;
            rdfs:label "test-mapping-delete" ;
            rml:logicalSource ex:logical_source ;
            rr:subjectMap [
                fnml:functionValue [
                    rr:predicateObjectMap [
                        rr:predicate fno:executes ;
                        rr:objectMap [ rr:constant idlab-fn:implicitDelete ]
                    ] ;
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:iri ;
                        rr:objectMap [ rr:template "http://example.org/{@id}" ]
                    ];
                    rr:predicateObjectMap [
                        rr:predicate idlab-fn:state ;
                        rr:objectMap [ rr:constant "/tmp/delete_state"; rr:dataType xsd:string; ]
                    ];
                ];
                rr:class <http://example.org/Entity>;
            ];
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicate ex:lifeCycleType ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:constant as:Delete ;
                    rr:termType rr:IRI
                ]
            ] .

        ex:pom_001 a rr:PredicateObjectMap ;
            rr:predicateMap [
                a rr:PredicateMap ;
                rr:constant rdfs:label
            ] ;
            rr:objectMap [
                a rr:ObjectMap ;
                rml:reference "@label" ;
                rr:termType rr:Literal
            ] .
    `;

    const RML_TM_REMOTE_SOURCE_AND_NO_TARGET = `
        ex:map_test-mapping_001 a rr:TriplesMap ;
        rdfs:label "test-mapping" ;
        rml:logicalSource [
            a rml:LogicalSource ;
            rml:source [
                a td:PropertyAffordance ;
                td:hasForm [
                    a td:Form ;
                    hctl:hasTarget "https://api.blue-bike.be/pub/location" ;
                    hctl:forContentType "application/json" ;
                    hctl:hasOperationType td:readproperty ;
                    htv:methodName "GET" ;
                    htv:headers ([
                        htv:fieldName "User-Agent" ;
                        htv:fieldValue "IDLab - Ghent University - imec (RMLMapper)"
                    ]);
                ]
            ] ;
            rml:referenceFormulation ql:JSONPath ;
            rml:iterator "$.[*]"
        ] ;
        rr:subjectMap [
            a rr:SubjectMap ;
            rr:template "https://blue-bike.be/stations/{id}" ;
            rr:class ex:BicycleParkingStation
        ] ;
        rr:predicateObjectMap [
            a rr:PredicateObjectMap ;
            rr:predicateMap [
                a rr:PredicateMap ;
                rr:constant ex:name
            ] ;
            rr:objectMap [
                a rr:ObjectMap ;
                rml:reference "name" ;
                rr:datatype xsd:string
            ]
        ] ;
        rr:predicateObjectMap [
            a rr:PredicateObjectMap ;
            rr:predicateMap [
                a rr:PredicateMap ;
                rr:constant ex:availableBikes
            ] ;
            rr:objectMap [
                a rr:ObjectMap ;
                rml:reference "bikes_available" ;
                rr:datatype xsd:integer
            ]
        ] .
    `;

    const LOCAL_RAW_DATA = `
        <resource>
            <data id="001" label="some data"></data>
            <data id="002" label="some other data"></data>
        </resource>
    `;

    const LOCAL_RAW_DATA_UPDATE = `
        <resource>
            <data id="001" label="some new data"></data>
            <data id="002" label="some other new data"></data>
        </resource>
    `;

    const LOCAL_RAW_DATA_YET_ANOTHER_UPDATE = `
        <resource>
            <data id="001" label="yet some more new data"></data>
            <data id="002" label="yet some other new data"></data>
        </resource>
    `;

    const LOCAL_SOURCE_1 = `
        <resource source_id="S001">
            <data source_id="S001" id="001" label="some data"></data>
            <data source_id="S001" id="002" label="some other data"></data>
        </resource>
    `;

    const LOCAL_SOURCE_2 = `
        <resource source_id="S002">
            <data source_id="S002" id="003" label="some data"></data>
            <data source_id="S002" id="004" label="some other data"></data>
        </resource>
    `;

    test("Mapping process with declared logical source and target", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET()}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const defaultOutputStream = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
        const mappingsPromise = new Promise<void>(resolve => {
            targetOutputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));
    
                expect(store.getQuads(null, null, null, null).length).toBe(4);
                expect(store.getQuads(
                    "http://example.org/001",
                    RDF.type,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                expect(store.getQuads(
                    "http://example.org/002",
                    RDFS.label,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                resolve();
            });
        });

        // Execute function
        await rmlMapper(mappingReader, defaultOutputStream, sources, targets, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Push raw input data
        await sourceInputStream.push(LOCAL_RAW_DATA);
        await mappingsPromise;
    });

    test("Mapping process with declared logical source and LDES target", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_LDES_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const defaultOutputStream = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
        const mappingsPromise = new Promise<void>(resolve => {
            targetOutputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));
    
                expect(store.getQuads(null, null, null, null).length).toBe(8);
                expect(store.getQuads(
                    null,
                    RDF.type,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(2);
                expect(store.getQuads(
                    null,
                    "http://purl.org/dc/terms/isVersionOf",
                    "http://example.org/001",
                    null).length
                ).toBe(1);
                expect(store.getQuads(
                    null,
                    "http://purl.org/dc/terms/isVersionOf",
                    "http://example.org/002",
                    null).length
                ).toBe(1);
                resolve();
            });
        });

        // Execute function
        await rmlMapper(mappingReader, defaultOutputStream, sources, targets, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Push raw input data
        await sourceInputStream.push(LOCAL_RAW_DATA);
        await mappingsPromise;
    });

    test("Mapping process with declared logical source data input arriving before mappings", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET()}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const defaultOutputStream = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
        const mappingsPromise = new Promise<void>(resolve => {
            targetOutputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));

                expect(store.getQuads(null, null, null, null).length).toBe(4);
                expect(store.getQuads(
                    "http://example.org/001",
                    RDF.type,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                expect(store.getQuads(
                    "http://example.org/002",
                    RDFS.label,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                resolve();
            });
        });

        // Execute function
        await rmlMapper(mappingReader, defaultOutputStream, sources, targets, "/tmp/rmlMapper.jar");

        // Push raw input data first
        await sourceInputStream.push(LOCAL_RAW_DATA);

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();
        await mappingsPromise;
    });

    test("Mapping process with multiple declared logical sources data input arriving before mappings", async () => {
        const rmlDoc1 = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET("dataset/data1.xml")}
        `;
        const rmlDoc2 = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET("dataset/data2.xml")}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const defaultOutputStream = new SimpleStream<string>();
        const sourceInputStream1 = new SimpleStream<string>();
        const sourceInputStream2 = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data1.xml",
                dataInput: sourceInputStream1,
                hasData: false,
                trigger: true
            },
            {
                location: "dataset/data2.xml",
                dataInput: sourceInputStream2,
                hasData: false,
                trigger: true
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
        let counter = 0;
        const mappingsPromise = new Promise<void>(resolve => {
            targetOutputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));
                expect(store.getQuads(null, null, null, null).length).toBe(4);
                expect(store.getQuads(
                    "http://example.org/001",
                    RDF.type,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                expect(store.getQuads(
                    "http://example.org/002",
                    RDFS.label,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                counter++;
                if (counter === 3) {
                    resolve();
                }
            });
        });

        // Execute function
        await rmlMapper(mappingReader, defaultOutputStream, sources, targets, "/tmp/rmlMapper.jar");

        // Push some data asynchronously
        await Promise.all([
            sourceInputStream1.push(LOCAL_RAW_DATA),
            sourceInputStream2.push(LOCAL_RAW_DATA)
        ]);
        // Push some mapping
        await mappingReader.push(rmlDoc1);
        // Push some more data
        await sourceInputStream1.push(LOCAL_RAW_DATA_UPDATE);
        await sourceInputStream2.push(LOCAL_RAW_DATA_UPDATE);
        await sourceInputStream1.push(LOCAL_RAW_DATA_YET_ANOTHER_UPDATE);
        await sourceInputStream2.push(LOCAL_RAW_DATA_YET_ANOTHER_UPDATE);
        // Finish pushing mappings input data
        await mappingReader.push(rmlDoc2);
        await mappingReader.end();
        await mappingsPromise;
    });

    test("Mapping process without any declared logical sources and using default output", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();

        const mappingsPromise = new Promise<void>(resolve => {
            outputStream.data(data => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));

                expect(store.getQuads(null, RDF.type, null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, "http://example.org/name", null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, "http://example.org/availableBikes", null, null).length).toBeGreaterThan(0);
                resolve();
            });
        });

        // Execute function
        await rmlMapper(mappingReader, outputStream, undefined, undefined, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();
        await mappingsPromise;
    });

    test("Mapping process with declared and undeclared logical sources", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET()}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;

        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];

        // Check output
        const mappingsPromise = new Promise<void>(resolve => {
            outputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));
                expect(store.getQuads(null, RDF.type, "http://example.org/Entity", null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, RDFS.label, null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, "http://example.org/name", null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, "http://example.org/availableBikes", null, null).length).toBeGreaterThan(0);
                resolve();
            });
        });

        // Execute function
        await rmlMapper(mappingReader, outputStream, sources, undefined, "/tmp/rmlMapper.jar");

        // Push raw input data first
        await sourceInputStream.push(LOCAL_RAW_DATA);

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();
        await mappingsPromise;
    });

    test("Mapping process with declared and undeclared logical sources and targets", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET()}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;

        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];

        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                writer: outputStream, // Here we are using the same stream as the default output
                data: ""
            }
        ];

        // Check output
        const mappingsPromise = new Promise<void>(resolve => {
            outputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));
                expect(store.getQuads(null, RDF.type, "http://example.org/Entity", null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, RDFS.label, null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, "http://example.org/name", null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(null, "http://example.org/availableBikes", null, null).length).toBeGreaterThan(0);
                expect(store.getQuads(
                    "http://example.org/001",
                    RDF.type,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                expect(store.getQuads(
                    "http://example.org/002",
                    RDFS.label,
                    null,
                    "http://example.org/myNamedGraph").length
                ).toBe(1);
                resolve();
            });
        });

        // Execute function
        await rmlMapper(mappingReader, outputStream, sources, targets, "/tmp/rmlMapper.jar");

        // Push raw input data first
        await sourceInputStream.push(LOCAL_RAW_DATA);

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();
        await mappingsPromise;
    });

    test("Mapping process with async input updates", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET()}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: true
            }
        ];

        // Check output
        let first = true;
        const mappingsPromise = new Promise<void>(resolve => {
            outputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));

                expect(store.getQuads(null, null, null, null).length).toBe(4);

                if (first) {
                    first = false;
                    expect(store.getQuads("http://example.org/001", RDFS.label, null, null)[0]
                        .object.value).toBe("some data");
                    expect(store.getQuads("http://example.org/002", RDFS.label, null, null)[0]
                        .object.value).toBe("some other data");
                } else {
                    expect(store.getQuads("http://example.org/001", RDFS.label, null, null)[0]
                        .object.value).toBe("some new data");
                    expect(store.getQuads("http://example.org/002", RDFS.label, null, null)[0]
                        .object.value).toBe("some other new data");
                    resolve();
                }
            });
        });

        // Execute function
        await rmlMapper(mappingReader, outputStream, sources, undefined, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Asynchronously push data updates
        sourceInputStream.push(LOCAL_RAW_DATA);
        await sourceInputStream.push(LOCAL_RAW_DATA_UPDATE);
        await mappingsPromise;
    });

    test("Mapping process with async input updates for multiple sources", async () => {
        const rmlDoc1 = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET("dataset/data1.xml")}
        `;
        const rmlDoc2 = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET("dataset/data2.xml")}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream1 = new SimpleStream<string>();
        const sourceInputStream2 = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data1.xml",
                dataInput: sourceInputStream1,
                hasData: false,
                trigger: true
            },
            {
                location: "dataset/data2.xml",
                dataInput: sourceInputStream2,
                hasData: false,
                trigger: true
            }
        ];

        // Check output
        let counter = 0;
        const mappingsPromise = new Promise<void>(resolve => {
            outputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));

                expect(store.getQuads(null, null, null, null).length).toBe(4);

                if (counter === 0) {
                    expect(store.getQuads("http://example.org/001", RDFS.label, null, null)[0]
                        .object.value).toBe("some data");
                    expect(store.getQuads("http://example.org/002", RDFS.label, null, null)[0]
                        .object.value).toBe("some other data");
                } else {
                    expect(store.getQuads("http://example.org/001", RDFS.label, null, null)[0]
                        .object.value).toBe("some new data");
                    expect(store.getQuads("http://example.org/002", RDFS.label, null, null)[0]
                        .object.value).toBe("some other new data");
                }
                counter++;
                if (counter === 2) {
                    resolve();
                }
            });
        });

        // Execute function
        await rmlMapper(mappingReader, outputStream, sources, undefined, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc1);
        await mappingReader.push(rmlDoc2);
        await mappingReader.end();

        // Asynchronously push data updates
        sourceInputStream1.push(LOCAL_RAW_DATA);
        sourceInputStream2.push(LOCAL_RAW_DATA);
        sourceInputStream1.push(LOCAL_RAW_DATA_UPDATE);
        await sourceInputStream2.push(LOCAL_RAW_DATA_UPDATE);
        await mappingsPromise;
    });

    test("Stateful mapping process with independent sources coming via the same logical source", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_STATEFUL}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: true,
                incRMLStateIndex: "source_id=\"([^\"]+)\""
            }
        ];


        let first = true;
        // Check output
        const mappingsPromise = new Promise<void>(resolve => {
            outputStream.data((data: string) => {
                const store = new Store();
                store.addQuads(new Parser().parse(data));
                if (first) {
                    first = false;
                    expect(store.getQuads("http://example.org/001", RDFS.label, null, null)[0]
                        .object.value).toBe("some data");
                    expect(store.getQuads("http://example.org/001", "http://example.org/lifeCycleType", null, null)[0]
                        .object.value).toBe(AS.Create);
                    expect(store.getQuads("http://example.org/002", RDFS.label, null, null)[0]
                        .object.value).toBe("some other data");
                    expect(store.getQuads("http://example.org/002", "http://example.org/lifeCycleType", null, null)[0]
                        .object.value).toBe(AS.Create);
                } else {
                    expect(store.getQuads("http://example.org/003", RDFS.label, null, null)[0]
                        .object.value).toBe("some data");
                    expect(store.getQuads("http://example.org/003", "http://example.org/lifeCycleType", null, null)[0]
                        .object.value).toBe(AS.Create);
                    expect(store.getQuads("http://example.org/004", RDFS.label, null, null)[0]
                        .object.value).toBe("some other data");
                    expect(store.getQuads("http://example.org/004", "http://example.org/lifeCycleType", null, null)[0]
                        .object.value).toBe(AS.Create);
                    resolve();
                }
            });
        });

        // Execute function
        await rmlMapper(mappingReader, outputStream, sources, undefined, "/tmp/rmlMapper.jar");

        // Push mappings
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Push data for first source
        await sourceInputStream.push(LOCAL_SOURCE_1);
        // Push data for second source
        await sourceInputStream.push(LOCAL_SOURCE_2);
        await mappingsPromise;
    });
});

function sleep(x: number): Promise<unknown> {
    return new Promise(resolve => setTimeout(resolve, x));
}

afterAll(async () => {
    // Clean up temporal files
    await deleteAsync([
        "/tmp/rml-*",
        "/tmp/create_state*",
        "/tmp/update_state*",
        "/tmp/delete_state*"
    ], { force: true });
});