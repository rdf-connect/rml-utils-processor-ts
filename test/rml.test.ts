import { describe, test, expect, afterAll } from "vitest";
import { FullProc } from "@rdfc/js-runner";
import { channel, createRunner } from "@rdfc/js-runner/lib/testUtils";
import { Parser } from "n3";
import { RdfStore } from "rdf-stores";
import { deleteAsync } from "del";
import { RMLMapperJS, Source, Target } from "../src/rml/rml";
import { AS, RDF, RDFS } from "../src/voc";
import { DF, TEST_LOGGER as logger, readChannel } from "./utils";

describe("Functional tests for the RMLMapperJS processor", () => {
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

    const RML_TM_LOCAL_SOURCE_AND_NO_TARGET = (source?: string) => {
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
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [targetOutputStream, targetOutput] = channel(runner, "targetOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");


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

        // Channel reader for the target output
        const targetOut = readChannel(targetOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Push raw input data and close channel
        await sourceInput.string(LOCAL_RAW_DATA);
        await sourceInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check default output
        const store = RdfStore.createDefault();
        new Parser().parse((await targetOut)[0]).forEach(quad => store.addQuad(quad));

        expect(store.getQuads(null, null, null, null).length).toBe(4);
        expect(store.getQuads(
            DF.namedNode("http://example.org/001"),
            RDF.terms.type,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(1);
        expect(store.getQuads(
            DF.namedNode("http://example.org/002"),
            RDFS.terms.label,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(1);
    });

    test("Mapping process with declared logical source and LDES target", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_LDES_TARGET}
        `;
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [targetOutputStream, targetOutput] = channel(runner, "targetOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");

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

        // Channel reader for the target output
        const targetOut = readChannel(targetOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Push raw input data and close channel
        await sourceInput.string(LOCAL_RAW_DATA);
        await sourceInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check target output
        const store = RdfStore.createDefault();
        new Parser().parse((await targetOut)[0]).forEach(quad => store.addQuad(quad));

        expect(store.getQuads(null, null, null, null).length).toBe(8);
        expect(store.getQuads(
            null,
            RDF.terms.type,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(2);
        expect(store.getQuads(
            null,
            DF.namedNode("http://purl.org/dc/terms/isVersionOf"),
            DF.namedNode("http://example.org/001"),
            null).length
        ).toBe(1);
        expect(store.getQuads(
            null,
            DF.namedNode("http://purl.org/dc/terms/isVersionOf"),
            DF.namedNode("http://example.org/002"),
            null).length
        ).toBe(1);
    });

    test("Mapping process with declared logical source data input arriving before mappings", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET()}
        `;
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [targetOutputStream, targetOutput] = channel(runner, "targetOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");

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

        // Channel reader for the target output
        const targetOut = readChannel(targetOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push raw input data first
        await sourceInput.string(LOCAL_RAW_DATA);
        await sourceInput.close();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check target output
        const store = RdfStore.createDefault();
        new Parser().parse((await targetOut)[0]).forEach(quad => store.addQuad(quad));

        expect(store.getQuads(null, null, null, null).length).toBe(4);
        expect(store.getQuads(
            DF.namedNode("http://example.org/001"),
            RDF.terms.type,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(1);
        expect(store.getQuads(
            DF.namedNode("http://example.org/002"),
            RDFS.terms.label,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(1);
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
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream] = channel(runner, "defaultOutput");
        const [targetOutputStream, targetOutput] = channel(runner, "targetOutput");
        const [sourceInput1, sourceInputStream1] = channel(runner, "dataInput1");
        const [sourceInput2, sourceInputStream2] = channel(runner, "dataInput2");

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

        // Channel reader for the target output
        const targetOut = readChannel(targetOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push some data asynchronously
        await Promise.all([
            sourceInput1.string(LOCAL_RAW_DATA),
            sourceInput2.string(LOCAL_RAW_DATA)
        ]);
        // Push some mapping
        await rmlInput.string(rmlDoc1);
        // Push some more data
        await sourceInput1.string(LOCAL_RAW_DATA_UPDATE);
        await sourceInput2.string(LOCAL_RAW_DATA_UPDATE);
        await sourceInput1.string(LOCAL_RAW_DATA_YET_ANOTHER_UPDATE);
        await sourceInput2.string(LOCAL_RAW_DATA_YET_ANOTHER_UPDATE);
        // Finish pushing mappings input data
        await rmlInput.string(rmlDoc2);
        await rmlInput.close();
        await sourceInput1.close();
        await sourceInput2.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check target outputs - expect 3 emissions
        const outputs = await targetOut;
        expect(outputs.length).toBeGreaterThanOrEqual(3);

        for (let i = 0; i < 3; i++) {
            const store = RdfStore.createDefault();
            new Parser().parse(outputs[i]).forEach(quad => store.addQuad(quad));
            expect(store.getQuads(null, null, null, null).length).toBe(4);
            expect(store.getQuads(
                DF.namedNode("http://example.org/001"),
                RDF.terms.type,
                null,
                DF.namedNode("http://example.org/myNamedGraph")).length
            ).toBe(1);
            expect(store.getQuads(
                DF.namedNode("http://example.org/002"),
                RDFS.terms.label,
                null,
                DF.namedNode("http://example.org/myNamedGraph")).length
            ).toBe(1);
        }
    });

    test("Mapping process without any declared logical sources and using default output", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");

        // Channel reader for the default output
        const defaultOut = readChannel(defaultOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources: undefined,
                targets: undefined,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check default output
        const store = RdfStore.createDefault();
        new Parser().parse((await defaultOut)[0]).forEach(quad => store.addQuad(quad));

        expect(store.getQuads(null, RDF.terms.type, null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, DF.namedNode("http://example.org/name"), null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, DF.namedNode("http://example.org/availableBikes"), null, null).length).toBeGreaterThan(0);
    });

    test("Mapping process with declared and undeclared logical sources", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET()}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;

        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");

        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];

        // Channel reader for the default output
        const defaultOut = readChannel(defaultOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets: undefined,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push raw input data first
        await sourceInput.string(LOCAL_RAW_DATA);
        await sourceInput.close();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check default output
        const store = RdfStore.createDefault();
        new Parser().parse((await defaultOut)[0]).forEach(quad => store.addQuad(quad));
        expect(store.getQuads(null, RDF.terms.type, DF.namedNode("http://example.org/Entity"), null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, RDFS.terms.label, null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, DF.namedNode("http://example.org/name"), null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, DF.namedNode("http://example.org/availableBikes"), null, null).length).toBeGreaterThan(0);
    });

    test("Mapping process with declared and undeclared logical sources and targets", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET()}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;

        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [targetOutputStream, targetOutput] = channel(runner, "targetOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");

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

        // Channel readers for outputs
        const targetOut = readChannel(targetOutput);
        const defaultOut = readChannel(defaultOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push raw input data first
        await sourceInput.string(LOCAL_RAW_DATA);
        await sourceInput.close();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check combined output (both target and default)
        const targetOutputs = await targetOut;
        const defaultOutputs = await defaultOut;

        // Combine all outputs for validation
        const allData = targetOutputs[0] + "\n" + defaultOutputs[0];
        const store = RdfStore.createDefault();
        new Parser().parse(allData).forEach(quad => store.addQuad(quad));

        expect(store.getQuads(null, RDF.terms.type, DF.namedNode("http://example.org/Entity"), null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, RDFS.terms.label, null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, DF.namedNode("http://example.org/name"), null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, DF.namedNode("http://example.org/availableBikes"), null, null).length).toBeGreaterThan(0);
        expect(store.getQuads(
            DF.namedNode("http://example.org/001"),
            RDF.terms.type,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(1);
        expect(store.getQuads(
            DF.namedNode("http://example.org/002"),
            RDFS.terms.label,
            null,
            DF.namedNode("http://example.org/myNamedGraph")).length
        ).toBe(1);
    });

    test("Mapping process with async input updates", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET()}
        `;
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");

        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: true
            }
        ];

        // Channel reader for the default output
        const defaultOut = readChannel(defaultOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets: undefined,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Asynchronously push data updates
        sourceInput.string(LOCAL_RAW_DATA);
        await sourceInput.string(LOCAL_RAW_DATA_UPDATE);
        await sourceInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check default outputs - expect 2 emissions
        const outputs = await defaultOut;
        expect(outputs.length).toBeGreaterThanOrEqual(2);

        // Check first output
        const store1 = RdfStore.createDefault();
        new Parser().parse(outputs[0]).forEach(quad => store1.addQuad(quad));
        expect(store1.getQuads(null, null, null, null).length).toBe(4);
        expect(store1.getQuads(DF.namedNode("http://example.org/001"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some data");
        expect(store1.getQuads(DF.namedNode("http://example.org/002"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some other data");

        // Check second output
        const store2 = RdfStore.createDefault();
        new Parser().parse(outputs[1]).forEach(quad => store2.addQuad(quad));
        expect(store2.getQuads(null, null, null, null).length).toBe(4);
        expect(store2.getQuads(DF.namedNode("http://example.org/001"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some new data");
        expect(store2.getQuads(DF.namedNode("http://example.org/002"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some other new data");
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
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [sourceInput1, sourceInputStream1] = channel(runner, "dataInput1");
        const [sourceInput2, sourceInputStream2] = channel(runner, "dataInput2");

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

        // Channel reader for the default output
        const defaultOut = readChannel(defaultOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets: undefined,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push mappings input data
        await rmlInput.string(rmlDoc1);
        await rmlInput.string(rmlDoc2);
        await rmlInput.close();

        // Asynchronously push data updates
        sourceInput1.string(LOCAL_RAW_DATA);
        sourceInput2.string(LOCAL_RAW_DATA);
        sourceInput1.string(LOCAL_RAW_DATA_UPDATE);
        await sourceInput2.string(LOCAL_RAW_DATA_UPDATE);
        await sourceInput1.close();
        await sourceInput2.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check default outputs - expect 2 emissions
        const outputs = await defaultOut;
        expect(outputs.length).toBeGreaterThanOrEqual(2);

        // Check first output
        const store1 = RdfStore.createDefault();
        new Parser().parse(outputs[0]).forEach(quad => store1.addQuad(quad));
        expect(store1.getQuads(null, null, null, null).length).toBe(4);
        expect(store1.getQuads(DF.namedNode("http://example.org/001"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some data");
        expect(store1.getQuads(DF.namedNode("http://example.org/002"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some other data");

        // Check second output
        const store2 = RdfStore.createDefault();
        new Parser().parse(outputs[1]).forEach(quad => store2.addQuad(quad));
        expect(store2.getQuads(null, null, null, null).length).toBe(4);
        expect(store2.getQuads(DF.namedNode("http://example.org/001"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some new data");
        expect(store2.getQuads(DF.namedNode("http://example.org/002"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some other new data");
    });

    test("Stateful mapping process with independent sources coming via the same logical source", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_STATEFUL}
        `;
        const runner = createRunner();

        // Define function parameters
        const [rmlInput, mappingReader] = channel(runner, "rml");
        const [defaultOutputStream, defaultOutput] = channel(runner, "defaultOutput");
        const [sourceInput, sourceInputStream] = channel(runner, "dataInput");

        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: true,
                incRMLStateIndex: "source_id=\"([^\"]+)\""
            }
        ];

        // Channel reader for the default output
        const defaultOut = readChannel(defaultOutput);

        // Define processor instance
        const proc = <FullProc<RMLMapperJS>>new RMLMapperJS(
            {
                mappingInput: mappingReader,
                defaultWriter: defaultOutputStream,
                sources,
                targets: undefined,
                jarLocation: "/tmp/rmlMapper.jar"
            },
            logger
        );

        // Execute processor
        await proc.init();
        const transformPromise = proc.transform();

        // Push mappings and close channel
        await rmlInput.string(rmlDoc);
        await rmlInput.close();

        // Push data for first source
        await sourceInput.string(LOCAL_SOURCE_1);
        // Push data for second source
        await sourceInput.string(LOCAL_SOURCE_2);
        await sourceInput.close();

        // Wait for mappings to be processed
        await transformPromise;

        // Check default outputs - expect 2 emissions
        const outputs = await defaultOut;
        expect(outputs.length).toBeGreaterThanOrEqual(2);

        // Check first output (first source)
        const store1 = RdfStore.createDefault();
        new Parser().parse(outputs[0]).forEach(quad => store1.addQuad(quad));
        expect(store1.getQuads(DF.namedNode("http://example.org/001"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some data");
        expect(store1.getQuads(DF.namedNode("http://example.org/001"), DF.namedNode("http://example.org/lifeCycleType"), null, null)[0]
            .object.value).toBe(AS.Create);
        expect(store1.getQuads(DF.namedNode("http://example.org/002"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some other data");
        expect(store1.getQuads(DF.namedNode("http://example.org/002"), DF.namedNode("http://example.org/lifeCycleType"), null, null)[0]
            .object.value).toBe(AS.Create);

        // Check second output (second source)
        const store2 = RdfStore.createDefault();
        new Parser().parse(outputs[1]).forEach(quad => store2.addQuad(quad));
        expect(store2.getQuads(DF.namedNode("http://example.org/003"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some data");
        expect(store2.getQuads(DF.namedNode("http://example.org/003"), DF.namedNode("http://example.org/lifeCycleType"), null, null)[0]
            .object.value).toBe(AS.Create);
        expect(store2.getQuads(DF.namedNode("http://example.org/004"), RDFS.terms.label, null, null)[0]
            .object.value).toBe("some other data");
        expect(store2.getQuads(DF.namedNode("http://example.org/004"), DF.namedNode("http://example.org/lifeCycleType"), null, null)[0]
            .object.value).toBe(AS.Create);
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