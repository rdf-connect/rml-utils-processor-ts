import { describe, test, expect, afterAll } from "@jest/globals";
import { SimpleStream } from "@ajuvercr/js-runner";
import { Parser, Store } from "n3";
import { deleteAsync } from "del";
import { rmlMapper, Source, Target } from "../src/rml/rml";
import { RDF, RDFS } from "../src/voc";

describe("Functional tests for the rmlMapper Connector Architecture function", async () => {
    const prefixes = `
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
@prefix ex: <http://example.org/> .`;

    test("Mapping process with declared logical sources and targets", async () => {
        const rmlDoc = `
            ${prefixes}
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

        const rawData = `
            <resource>
                <data id="001" label="some data"></data>
                <data id="002" label="some other data"></data>
            </resource>
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
                writer: targetOutputStream
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
        await sourceInputStream.push(rawData);
    });

    test("Mapping process without declared logical sources and default output", async () => {
        const rmlDoc = `
            ${prefixes}
            ex:map_test-mapping_000 a rr:TriplesMap ;
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
});

afterAll(async () => {
    // Clean up temporal files
    await deleteAsync(["/tmp/rml*"], { force: true });
});