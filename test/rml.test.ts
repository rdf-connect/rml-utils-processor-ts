import { describe, test, expect, afterAll } from "@jest/globals";
import { SimpleStream } from "@ajuvercr/js-runner";
import { Parser, Store } from "n3";
import { deleteAsync } from "del";
import { rmlMapper, Source, Target } from "../src/rml/rml";
import { RDF, RDFS } from "../src/voc";

describe("Functional tests for the rmlMapper Connector Architecture function", () => {
    const PREFIXES = `
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix rml: <http://semweb.mmlab.be/ns/rml#> .
        @prefix rmlt: <http://semweb.mmlab.be/ns/rml-target#> .
        @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
        @prefix void: <http://rdfs.org/ns/void#> .
        @prefix formats: <http://www.w3.org/ns/formats/> .
        @prefix td: <https://www.w3.org/2019/wot/td#> .
        @prefix htv: <http://www.w3.org/2011/http#> .
        @prefix hctl: <https://www.w3.org/2019/wot/hypermedia#> .
        @prefix ldes: <https://w3id.org/ldes#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix ex: <http://example.org/> .
    `;

    const RML_TM_LOCAL_SOURCE_AND_TARGET = `
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

    const RML_TM_LOCAL_SOURCE_AND_NO_TARGET = `
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

    test("Mapping process with declared logical source and target", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                newLocation: "",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                newLocation: "",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
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
        });

        // Execute function
        await rmlMapper(mappingReader, sources, targets, undefined, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Push raw input data
        await sourceInputStream.push(LOCAL_RAW_DATA);
    });

    test("Mapping process with declared logical source and LDES target", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_LDES_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                newLocation: "",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                newLocation: "",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
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
        });

        // Execute function
        await rmlMapper(mappingReader, sources, targets, undefined, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Push raw input data
        await sourceInputStream.push(LOCAL_RAW_DATA);
    });

    test("Mapping process with declared logical source data input arriving before mappings", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const targetOutputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                newLocation: "",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];
        const targets: Target[] = [
            {
                location: "file:///results/output.nq",
                newLocation: "",
                writer: targetOutputStream,
                data: ""
            }
        ];

        // Check output
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
        });

        // Execute function
        await rmlMapper(mappingReader, sources, targets, undefined, "/tmp/rmlMapper.jar");

        // Push raw input data first
        await sourceInputStream.push(LOCAL_RAW_DATA);

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();
    });

    test("Mapping process without any declared logical sources and using default output", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();

        outputStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            expect(store.getQuads(null, RDF.type, null, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, "http://example.org/name", null, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, "http://example.org/availableBikes", null, null).length).toBeGreaterThan(0);
        });

        // Execute function
        await rmlMapper(mappingReader, undefined, undefined, outputStream, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

    });

    test("Mapping process with declared and undeclared logical sources", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET}
            ${RML_TM_REMOTE_SOURCE_AND_NO_TARGET}
        `;

        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                newLocation: "",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: false
            }
        ];

        // Check output
        outputStream.data((data: string) => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));
            expect(store.getQuads(null, RDF.type, "http://example.org/Entity", null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, RDFS.label, null, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, "http://example.org/name", null, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, "http://example.org/availableBikes", null, null).length).toBeGreaterThan(0);
        });

        // Execute function
        await rmlMapper(mappingReader, sources, undefined, outputStream, "/tmp/rmlMapper.jar");

        // Push raw input data first
        await sourceInputStream.push(LOCAL_RAW_DATA);

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();
    });

    test("Mapping process with async input updates", async () => {
        const rmlDoc = `
            ${PREFIXES}
            ${RML_TM_LOCAL_SOURCE_AND_NO_TARGET}
        `;
        // Define function parameters
        const mappingReader = new SimpleStream<string>();
        const sourceInputStream = new SimpleStream<string>();
        const outputStream = new SimpleStream<string>();
        const sources: Source[] = [
            {
                location: "dataset/data.xml",
                newLocation: "",
                dataInput: sourceInputStream,
                hasData: false,
                trigger: true
            }
        ];

        // Check output
        let first = true;
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
            }
        });

        // Execute function
        await rmlMapper(mappingReader, sources, undefined, outputStream, "/tmp/rmlMapper.jar");

        // Push mappings input data
        await mappingReader.push(rmlDoc);
        await mappingReader.end();

        // Asynchronously push data updates
        sourceInputStream.push(LOCAL_RAW_DATA);
        await sleep(1000);
        await sourceInputStream.push(LOCAL_RAW_DATA_UPDATE);
        await sleep(3000);
    });
});

function sleep(x: number): Promise<unknown> {
    return new Promise(resolve => setTimeout(resolve, x));
}

afterAll(async () => {
    // Clean up temporal files
    await deleteAsync(["/tmp/rml*"], { force: true });
});