import { describe, test, expect } from "@jest/globals";
import { SimpleStream } from "@rdfc/js-runner";
import { Parser, Store, DataFactory, Quad, OTerm } from "n3";
import { RDF, RR, RML, RMLT, IDLAB_FN, AS, FNML, FNO, GREL } from "../src/voc";
import { BASE, rml2incrml, IncRMLConfig } from "../src/rml/incrml";
import { Quad_Object, Quad_Subject } from "@rdfjs/types";

describe("Functional tests for the rml2incrml Connector Architecture function", () => {
    const PREFIXES = `
        @prefix rdf: <${RDF.namespace}> .
        @prefix rr: <${RR.namespace}> .
        @prefix rml: <${RML.namespace}> .
        @prefix rmlt: <${RMLT.namespace}> .
        @prefix fnml: <${FNML.namespace}> .
        @prefix fno: <${FNO.namespace}> .
        @prefix idlab-fn: <${IDLAB_FN.namespace}> .
        @prefix grel: <${GREL.namespace}> .
        @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
    `;
    const TM = (i, source, template, graph?, clazz?) => {
        return `
            <http://ex.org/m${i}> a rr:TriplesMap ;
                rml:logicalSource [
                    a rml:LogicalSource ;
                    rml:source "${source}" ;
                    rml:iterator "//Data" ;
                    rml:referenceFormulation ql:XPath
                ] ;
                rr:subjectMap [
                    a rr:SubjectMap ;
                    rr:template "${template}" ;
                    ${clazz ? `rr:class ${clazz} ;` : ""}
                    ${!graph ? "" : `
                    rr:graphMap [
                        a rr:GraphMap ;
                        rr:constant ${graph}
                    ]`}
                ]
        `;
    };
    const TM2 = (i, source, template) => {
        return `
            <http://ex.org/m${i}> a rr:TriplesMap ;
                rml:logicalSource [
                    a rml:LogicalSource ;
                    rml:source "${source}" ;
                    rml:referenceFormulation ql:CSV
                ] ;
                rr:subjectMap [
                    a rr:SubjectMap ;
                    rr:template "${template}"
                ]
        `;
    };
    const TM_FN = (i, source, template, graph?, clazz?) => {
        return `
            <http://ex.org/ls0> a rml:LogicalSource ;
                rml:source "${source}" ;
                rml:iterator "//Data" ;
                rml:referenceFormulation ql:XPath .

            <http://ex.org/m${i}> a rr:TriplesMap ;
                rml:logicalSource <http://ex.org/ls0> ;
                rr:subjectMap [
                    a rr:FunctionTermMap ;
                    fnml:functionValue [
                        rml:logicalSource <http://ex.org/ls0> ;
                        rr:predicateObjectMap [
                            rr:predicate fno:executes ;
                            rr:objectMap [
                                rr:constant idlab-fn:trueCondition ;
                                rr:termType rr:IRI
                            ]
                        ] ;
                        rr:predicateObjectMap [
                            rr:predicateMap [ rr:constant idlab-fn:strBoolean ] ;
                            rr:objectMap [
                                a fnml:FunctionTermMap ;
                                fnml:functionValue [
                                    rml:logicalSource <http://ex.org/ls0> ;
                                    rr:predicateObjectMap [
                                        rr:predicate fno:executes ;
                                        rr:objectMap [
                                            rr:constant idlab-fn:notEqual ;
                                            rr:termType rr:IRI
                                        ]
                                    ] ;
                                    rr:predicateObjectMap [
                                        rr:predicate grel:valueParameter ;
                                        rr:objectMap [
                                            rml:reference "Property/@Value" ;
	                                        rr:termType rr:Literal
                                        ]
                                    ] ;
                                    rr:predicateObjectMap [
                                        rr:predicate grel:valueParameter2 ;
                                        rr:objectMap [
                                            rr:constant "" ;
	                                        rr:termType rr:Literal
                                        ]
                                    ]
                                ]
                            ]
                        ] ;
                        rr:predicateObjectMap [
                            rr:predicate idlab-fn:str ;
                            rr:objectMap [ rr:template "${template}" ]
                        ] ;

                    ] ;
                    ${clazz ? `rr:class ${clazz} ;` : ""}
                    ${!graph ? "" : `
                    rr:graphMap [
                        a rr:GraphMap ;
                        rr:constant ${graph}
                    ]`}
                ]
        `;
    };
    const POM = (predicate, { pred, obj }) => {
        return `
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant <${predicate}>
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    ${pred} ${obj}
                ]
            ]
        `;
    };

    const POM_JOIN = (predicate, parentTM) => {
        return `
            rr:predicateObjectMap [
                a rr:PredicateObjectMap ;
                rr:predicateMap [
                    a rr:PredicateMap ;
                    rr:constant <${predicate}>
                ] ;
                rr:objectMap [
                    a rr:ObjectMap ;
                    rr:parentTriplesMap <${parentTM}> ;
                    rml:joinCondition [
                        a fnml:FunctionTermMap ;
                        fnml:functionValue [
                            rr:predicateObjectMap [
                                rr:predicate fno:executes ;
                                rr:objectMap [
                                    rr:constant idlab-fn:equal ;
                                    rr:termType rr:IRI
                                ]
                            ] ;
                            rr:predicateObjectMap [
                                rr:predicate grel:valueParameter ;
                                rr:objectMap [
                                    rml:reference "Property/@Value" ;
                                    rr:termType rr:Literal
                                ]
                            ] ;
                            rr:predicateObjectMap [
                                rr:predicate grel:valueParameter2 ;
                                rr:objectMap [
                                    rr:parentTermMap [
                                        rml:reference "Property1/@Value"
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        `;
    };

    test("1 RML mapping with 1 Triples Map without explicit target", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            // Check there are 3 Triples Maps
            expect(store.getQuads(null, RDF.type, RR.TriplesMap, null).length).toBe(3);

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(3);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null)[0]).toBeDefined();

            // Check that the watched properties template is properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value)"),
                null
            )[0]).toBeDefined();
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>")};
            ${POM("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", { pred: "rr:constant", obj: "<http://ex.org/ns/SomeClass>" })};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 Triples Map without explicit target", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const store = new Store();
        incrmlStream.data(data => {
            store.addQuads(new Parser().parse(data));

            // Check there are 6 Triples Maps
            expect(store.getQuads(null, RDF.type, RR.TriplesMap, null).length).toBe(6);

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(6);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null).length).toBe(2);

            // Check that the watched properties template is properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value)"),
                null
            )[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, DataFactory.literal("prop0=Column2"), null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, DataFactory.literal("prop1=Column3"), null)[0]).toBeDefined();
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })}.

            ${TM2(1, "dataset/data.csv", "http://ex.org/instances/{Column1}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{Column2}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rml:reference", obj: "\"Column3\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 Triples Map doing a join and without explicit target", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            // Check there are 4 Triples Maps (including the join TM)
            expect(store.getQuads(null, RDF.type, RR.TriplesMap, null).length).toBe(4);

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(3);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null)[0]).toBeDefined();

            // Check that the watched properties template is properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value)"),
                null
            )[0]).toBeDefined();
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM_JOIN("http://ex.org/ns/joinProperty", "http://ex.org/m1")}.

            ${TM(1, "dataset/data2.xml", "http://ex.org/instances/{Property1/@Value}")}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 Triples Maps (same source, same template, same named graph) with versioned IRIs", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            },
            targetConfig: {
                targetPath: "./output.ttl",
                timestampPath: "http://purl.org/dc/terms/modified",
                versionOfPath: "http://purl.org/dc/terms/isVersionOf",
                serialization: "http://www.w3.org/ns/formats/Turtle",
                uniqueIRIs: true
            }
        };

        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            // Check that Logical Target triples are defined
            expect(store.getQuads(`${BASE}LDES_LT`, null, null, null).length).toBe(5);

            // Check there are 3 Triples Maps
            const tms = store.getQuads(null, RDF.type, RR.TriplesMap, null);
            expect(tms.length).toBe(3);

            // Check that each associated Subject Map is linked to the proper Logical Target
            tms.forEach(tm => {
                const smQ = store.getQuads(tm.subject, RR.subjectMap, null, null)[0];
                expect(store.getQuads(smQ.object, RML.logicalTarget, `${BASE}LDES_LT`, null)[0]).toBeDefined();
            });

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(3);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null)[0]).toBeDefined();
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null)[0]).toBeDefined();

            // Check that the watched properties template is properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || YetAnotherProperty/@Value)"),
                null
            )[0]).toBeDefined();
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })}.

            ${TM(1, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{YetAnotherProperty/@Value}\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 Triples Maps (same source, same template, different named graph) with versioned IRIs", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            },
            targetConfig: {
                targetPath: "./output.ttl",
                timestampPath: "http://purl.org/dc/terms/modified",
                versionOfPath: "http://purl.org/dc/terms/isVersionOf",
                serialization: "http://www.w3.org/ns/formats/Turtle",
                uniqueIRIs: true
            }
        };

        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            // Check that Logical Target triples are defined
            expect(store.getQuads(`${BASE}LDES_LT`, null, null, null).length).toBe(5);

            // Check there are 6 Triples Maps
            const tms = store.getQuads(null, RDF.type, RR.TriplesMap, null);
            expect(tms.length).toBe(6);

            // Check that each associated Subject Map is linked to the proper Logical Target
            tms.forEach(tm => {
                const smQ = store.getQuads(tm.subject, RR.subjectMap, null, null)[0];
                expect(store.getQuads(smQ.object, RML.logicalTarget, `${BASE}LDES_LT`, null)[0]).toBeDefined();
            });

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(6);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null).length).toBe(2);

            // Check that states are different for all Triples Maps
            const statePOMs = store.getSubjects(RR.predicate, IDLAB_FN.state, null);
            const states: string[] = [];
            statePOMs.forEach(pom => {
                const om = store.getObjects(pom, RR.objectMap, null)[0];
                states.push(store.getObjects(om, RR.constant, null)[0].value);
            });
            expect(new Set(states).size).toBe(states.length);

            // Check that the watched properties templates are properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value)"),
                null
            )[0]).toBeDefined();
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || YetAnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
                null
            )[0]).toBeDefined();
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph1>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })}.

            ${TM(1, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph2>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{YetAnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/type", { pred: "rml:reference", obj: "\"@Name\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 3 Triples Maps (same source, different template, no named graph) without explicit target", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const store = new Store();
        incrmlStream.data(data => {
            store.addQuads(new Parser().parse(data));

            // Check there are 9 Triples Maps
            const tms = store.getQuads(null, RDF.type, RR.TriplesMap, null);
            expect(tms.length).toBe(9);

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(9);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null).length).toBe(3);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null).length).toBe(3);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null).length).toBe(3);

            // Check that states are different for all Triples Maps
            const statePOMs = store.getQuads(null, RR.predicate, IDLAB_FN.state, null);
            const states: string[] = [];
            statePOMs.forEach(pom => {
                const om = store.getQuads(pom.subject, RR.objectMap, null, null)[0].object;
                states.push(store.getQuads(om, RR.constant, null, null)[0].object.value);
            });
            expect(new Set(states).size).toBe(states.length);

            // Check that the watched properties templates are properly defined
            expect(store.getQuads(null, RML.reference, DataFactory.literal(""), null).length).toBe(3);
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property1/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:constant", obj: "<http://ex.org/ns/SomeClass>" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })}.

            ${TM(1, "dataset/data.xml", "http://ex.org/instances/{Property2/@Value}")};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rr:constant", obj: "\"Some Other Value\"" })}.

            ${TM(2, "dataset/data.xml", "http://ex.org/instances/{Property3/@Value}", null, "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })}.

        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 Triples Maps (different source, same template, no named graph) with versioned IRIs", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            },
            targetConfig: {
                targetPath: "./output.ttl",
                timestampPath: "http://purl.org/dc/terms/modified",
                versionOfPath: "http://purl.org/dc/terms/isVersionOf",
                serialization: "http://www.w3.org/ns/formats/Turtle",
                uniqueIRIs: true
            }
        };

        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            // Check that Logical Target triples are defined
            expect(store.getQuads(`${BASE}LDES_LT`, null, null, null).length).toBe(5);

            // Check there are 6 Triples Maps
            const tms = store.getQuads(null, RDF.type, RR.TriplesMap, null);
            expect(tms.length).toBe(6);

            // Check that each associated Subject Map is linked to the proper Logical Target
            tms.forEach(tm => {
                const smQ = store.getQuads(tm.subject, RR.subjectMap, null, null)[0];
                expect(store.getQuads(smQ.object, RML.logicalTarget, `${BASE}LDES_LT`, null)[0]).toBeDefined();
            });

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(6);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null).length).toBe(2);

            // Check that states are different for all Triples Maps
            const statePOMs = store.getQuads(null, RR.predicate, IDLAB_FN.state, null);
            const states: string[] = [];
            statePOMs.forEach(pom => {
                const om = store.getQuads(pom.subject, RR.objectMap, null, null)[0].object;
                states.push(store.getQuads(om, RR.constant, null, null)[0].object.value);
            });
            expect(new Set(states).size).toBe(states.length);

            // Check that the watched properties templates are properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || SomeProperty/@Name)"),
                null
            )[0]).toBeDefined();
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
                null
            )[0]).toBeDefined();
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data1.xml", "http://ex.org/instances/{Property/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/type", { pred: "rml:reference", obj: "\"SomeProperty/@Name\"" })}.

            ${TM(1, "dataset/data2.xml", "http://ex.org/instances/{Property/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/type", { pred: "rml:reference", obj: "\"@Name\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 (Fn) Triples Maps (same source, different template, no named graph) without explicit target", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const store = new Store();
        incrmlStream.data(data => {
            store.addQuads(new Parser().parse(data));

            // Check there are 6 Triples Maps
            const tms = store.getSubjects(RDF.type, RR.TriplesMap, null);
            expect(tms.length).toBe(6);

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(6);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null).length).toBe(2);

            // Check that states are different for all Triples Maps
            const statePOMs = store.getQuads(null, RR.predicate, IDLAB_FN.state, null);
            const states: string[] = [];
            statePOMs.forEach(pom => {
                const om = store.getQuads(pom.subject, RR.objectMap, null, null)[0].object;
                states.push(store.getQuads(om, RR.constant, null, null)[0].object.value);
            });
            expect(new Set(states).size).toBe(states.length);

            // Check that the watched properties templates are properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || SomeProperty/@Name)"),
                null
            )[0]).toBeDefined();
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || YetAnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
                null
            )[0]).toBeDefined();

            // Check that conditional function is embedded correctly on stateful mappings
            tms.forEach(tm => {
                const sm = store.getObjects(tm, RR.subjectMap, null)[0];
                const fv = store.getObjects(sm, FNML.functionValue, null)[0];
                const poms = store.getObjects(fv, RR.predicateObjectMap, null);
                expect(poms.length).toBeGreaterThanOrEqual(3);
                // Check the IRI template param is the result of an embedded function
                const iriPom = poms.find(pom => {
                    const iriPm = store.getObjects(pom, RR.predicate, null)[0];
                    return iriPm.value === IDLAB_FN.iri;
                });
                if (iriPom) {
                    const iriOm = store.getObjects(iriPom, RR.objectMap, null)[0];
                    const iriFnTm = store.getObjects(iriOm, FNML.functionValue, null)[0];
                    if (iriFnTm) {
                        const iriFnPoms = store.getObjects(iriFnTm, RR.predicateObjectMap, null);
                        expect(iriFnPoms.some(pom => {
                            const exec = store.getObjects(pom, RR.predicate, null)[0].value
                            const fnObj = store.getObjects(pom, RR.objectMap, null)[0];
                            const fn = store.getObjects(fnObj, RR.constant, null)[0];
                            if (fn) {
                                return exec === FNO.executes && fn.value === IDLAB_FN.trueCondition;
                            }
                        })).toBeTruthy();
                    }
                }
            });
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM_FN(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/pred1", { pred: "rml:reference", obj: "\"SomeProperty/@Name\"" })}.

            ${TM(1, "dataset/data.xml", "http://ex.org/instances/{Property1/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{YetAnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/type", { pred: "rml:reference", obj: "\"@Name\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("1 RML mapping with 2 (Fn) Triples Maps (different source, different template, same named graph) with versioned IRIs", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            },
            targetConfig: {
                targetPath: "./output.ttl",
                timestampPath: "http://purl.org/dc/terms/modified",
                versionOfPath: "http://purl.org/dc/terms/isVersionOf",
                serialization: "http://www.w3.org/ns/formats/Turtle",
                uniqueIRIs: true
            }
        };

        const store = new Store();
        incrmlStream.data(data => {
            store.addQuads(new Parser().parse(data));

            // Check that Logical Target triples are defined
            expect(store.getQuads(`${BASE}LDES_LT`, null, null, null).length).toBe(5);

            // Check there are 6 Triples Maps
            const tms = store.getSubjects(RDF.type, RR.TriplesMap, null);
            expect(tms.length).toBe(6);

            // Check that each associated Subject Map is linked to the proper Logical Target
            tms.forEach(tm => {
                const smQ = store.getQuads(tm, RR.subjectMap, null, null)[0];
                expect(store.getQuads(smQ.object, RML.logicalTarget, `${BASE}LDES_LT`, null)[0]).toBeDefined();
            });

            // Check there are Object Maps pointing to lifecycle entities
            expect(store.getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null).length).toBe(6);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null).length).toBe(2);
            expect(store.getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null).length).toBe(2);

            // Check that states are different for all Triples Maps
            const statePOMs = store.getQuads(null, RR.predicate, IDLAB_FN.state, null);
            const states: string[] = [];
            statePOMs.forEach(pom => {
                const om = store.getQuads(pom.subject, RR.objectMap, null, null)[0].object;
                states.push(store.getQuads(om, RR.constant, null, null)[0].object.value);
            });
            expect(new Set(states).size).toBe(states.length);

            // Check that the watched properties templates are properly defined
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || SomeProperty/@Name)"),
                null
            )[0]).toBeDefined();
            expect(store.getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || YetAnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
                null
            )[0]).toBeDefined();

            // Check that conditional function is embedded correctly on stateful mappings
            tms.forEach(tm => {
                const sm = store.getObjects(tm, RR.subjectMap, null)[0];
                const fv = store.getObjects(sm, FNML.functionValue, null)[0];
                const poms = store.getObjects(fv, RR.predicateObjectMap, null);
                expect(poms.length).toBeGreaterThanOrEqual(3);
                // Check the IRI template param is the result of an embedded function
                const iriPom = poms.find(pom => {
                    const iriPm = store.getObjects(pom, RR.predicate, null)[0];
                    return iriPm.value === IDLAB_FN.iri;
                });
                if (iriPom) {
                    const iriOm = store.getObjects(iriPom, RR.objectMap, null)[0];
                    const iriFnTm = store.getObjects(iriOm, FNML.functionValue, null)[0];
                    const iriFnPoms = store.getObjects(iriFnTm, RR.predicateObjectMap, null);
                    expect(iriFnPoms.some(pom => {
                        const exec = store.getObjects(pom, RR.predicate, null)[0].value
                        const fnObj = store.getObjects(pom, RR.objectMap, null)[0];
                        const fn = store.getObjects(fnObj, RR.constant, null)[0];
                        if (fn) {
                            return exec === FNO.executes && fn.value === IDLAB_FN.trueCondition;
                        }
                    })).toBeTruthy();
                }
            });
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM_FN(0, "dataset/data1.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>",)};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/pred1", { pred: "rml:reference", obj: "\"SomeProperty/@Name\"" })}.

            ${TM_FN(1, "dataset/data2.xml", "http://ex.org/instances/{Property1/@Value}", "<http://ex.org/myGraph>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{YetAnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/type", { pred: "rml:reference", obj: "\"@Name\"" })}.
        `;
        await rmlStream.push(mapping);
    });

    test("2 RML mappings with 2 Triples Map (different source, different template, no named graph) without explicit target", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const stores: Array<Store> = [];
        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data))
            stores.push(store);
        }).on("end", () => {
            // Check there are 6 Triples Maps
            expect(getQuads(null, RDF.type, RR.TriplesMap, null, stores).length).toBe(6);


            // Check there are Object Maps pointing to lifecycle entities
            expect(getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null, stores).length).toBe(6);
            expect(getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null, stores).length).toBe(2);
            expect(getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null, stores).length).toBe(2);
            expect(getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null, stores).length).toBe(2);

            // Check that states are different for all Triples Maps
            const statePOMs = getSubjects(RR.predicate, IDLAB_FN.state, null, stores);
            const stateSet = new Set<string>();
            statePOMs.forEach(pom => {
                const oms = getObjects(pom, RR.objectMap, null, stores);
                oms.forEach(om => {
                    getObjects(om, RR.constant, null, stores).forEach(state => stateSet.add(state.value));
                });
            });
            expect(stateSet.size).toBe(6);

            // Check that the watched properties templates are properly defined
            expect(getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty1/@Value || '&' || 'prop1=' || YetAnotherProperty1/@Value)"),
                null,
                stores
            )[0]).toBeDefined();
            expect(getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty2/@Value || '&' || 'prop1=' || YetAnotherProperty2/@Value)"),
                null,
                stores
            )[0]).toBeDefined();
        });


        await rml2incrml(rmlStream, config, incrmlStream);

        // Push mappings
        const mapping1 = `
            ${PREFIXES}
            ${TM(0, "dataset/data1.xml", "http://ex.org/instances/{Property1/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty1/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty1/@Value\"" })}.
        `;
        const mapping2 = `
            ${PREFIXES}
            ${TM(0, "dataset/data2.xml", "http://ex.org/instances/{Property2/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty2/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty2/@Value\"" })}.
        `;

        await Promise.all([
            rmlStream.push(mapping1),
            rmlStream.push(mapping2)
        ]);
        await rmlStream.end();
    });

    test("4 RML mapping with 8 (Fn) Triples Maps (different/same source, different/same template, different/same named graph) with versioned IRIs", async () => {
        const rmlStream = new SimpleStream<string>();
        const incrmlStream = new SimpleStream<string>();
        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            },
            targetConfig: {
                targetPath: "./output.ttl",
                timestampPath: "http://purl.org/dc/terms/modified",
                versionOfPath: "http://purl.org/dc/terms/isVersionOf",
                serialization: "http://www.w3.org/ns/formats/Turtle",
                uniqueIRIs: true
            }
        };

        const stores: Array<Store> = [];
        incrmlStream.data(data => {
            const store = new Store();
            store.addQuads(new Parser().parse(data))
            stores.push(store);
        }).on("end", () => {
            // Check that Logical Target triples are defined
            expect(getQuads(`${BASE}LDES_LT`, null, null, null, stores).length).toBe(20);

            // Check there are 18 Triples Maps
            const tms = getSubjects(RDF.type, RR.TriplesMap, null, stores);
            expect(tms.length).toBe(21);

            // Check that each associated Subject Map is linked to the proper Logical Target
            tms.forEach(tm => {
                const smQ = getQuads(tm, RR.subjectMap, null, null, stores)[0];
                expect(getQuads(smQ.object, RML.logicalTarget, `${BASE}LDES_LT`, null, stores)[0]).toBeDefined();
            });

            // Check there are Object Maps pointing to lifecycle entities
            expect(getQuads(null, RR.predicate, config.lifeCycleConfig.predicate, null, stores).length).toBe(21);
            expect(getQuads(null, RR.constant, config.lifeCycleConfig.create.type, null, stores).length).toBe(7);
            expect(getQuads(null, RR.constant, config.lifeCycleConfig.update.type, null, stores).length).toBe(7);
            expect(getQuads(null, RR.constant, config.lifeCycleConfig.delete.type, null, stores).length).toBe(7);

            // Check that states are different for all Triples Maps
            const statePOMs = getSubjects(RR.predicate, IDLAB_FN.state, null, stores);
            const stateSet = new Set<string>();
            statePOMs.forEach(pom => {
                const oms = getObjects(pom, RR.objectMap, null, stores);
                oms.forEach(om => {
                    getObjects(om, RR.constant, null, stores).forEach(state => stateSet.add(state.value));
                });
            });
            expect(stateSet.size).toBe(18);

            // Check that the watched properties templates are properly defined
            expect(getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty1/@Value || '&' || 'prop1=' || YetAnotherProperty1/@Value)"),
                null,
                stores
            )[0]).toBeDefined();
            expect(getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty2/@Value || '&' || 'prop1=' || YetAnotherProperty2/@Value)"),
                null,
                stores
            )[0]).toBeDefined();
            expect(getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty2.5/@Value || '&' || 'prop1=' || YetAnotherProperty2.5/@Value)"),
                null,
                stores
            )[0]).toBeDefined();
            const wp3 = getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty3/@Value || '&' || 'prop1=' || YetAnotherProperty3/@Value)"),
                null,
                stores
            );
            expect(wp3.length).toBe(2);
            expect(getQuads(
                null,
                RML.reference,
                DataFactory.literal("('prop0=' || AnotherProperty4/@Value || '&' || 'prop1=' || YetAnotherProperty4/@Value || '&' || 'prop2=' || AnotherProperty1/@Value || '&' || 'prop3=' || YetAnotherProperty1/@Value)"),
                null,
                stores
            )[0]).toBeDefined();

            // Check that conditional function is embedded correctly on stateful mappings
            tms.forEach(tm => {
                const sm = getObjects(tm, RR.subjectMap, null, stores)[0];
                const fv = getObjects(sm, FNML.functionValue, null, stores)[0];
                const poms = getObjects(fv, RR.predicateObjectMap, null, stores);
                expect(poms.length).toBeGreaterThanOrEqual(3);
                // Check the IRI template param is the result of an embedded function
                const iriPom = poms.find(pom => {
                    const iriPm = getObjects(pom, RR.predicate, null, stores)[0];
                    return iriPm.value === IDLAB_FN.iri;
                });
                if (iriPom) {
                    const iriOm = getObjects(iriPom, RR.objectMap, null, stores)[0];
                    const iriFnTm = getObjects(iriOm, FNML.functionValue, null, stores)[0];
                    if (iriFnTm) {
                        const iriFnPoms = getObjects(iriFnTm, RR.predicateObjectMap, null, stores);
                        expect(iriFnPoms.some(pom => {
                            const exec = getObjects(pom, RR.predicate, null, stores)[0].value
                            const fnObj = getObjects(pom, RR.objectMap, null, stores)[0];
                            const fn = getObjects(fnObj, RR.constant, null, stores)[0];
                            if (fn) {
                                return exec === FNO.executes && fn.value === IDLAB_FN.trueCondition;
                            }
                        })).toBeTruthy();
                    }
                }
            });
        });

        await rml2incrml(rmlStream, config, incrmlStream);

        // Push mappings
        const mapping1 = `
            ${PREFIXES}
            ${TM(0, "dataset/data1.xml", "http://ex.org/instances/{Property1/@Value}", "<http://ex.org/graph1>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty1/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty1/@Value\"" })}.
        `;
        const mapping2 = `
            ${PREFIXES}
            ${TM(0, "dataset/data2.xml", "http://ex.org/instances/{Property2/@Value}", "<http://ex.org/graph1>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty2/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty2/@Value\"" })}.

            ${TM_FN(1, "dataset/data2.xml", "http://ex.org/instances/{Property2.5/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty2.5/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty2.5/@Value\"" })}.
        `;
        const mapping3 = `
            ${PREFIXES}
            ${TM_FN(0, "dataset/data1.xml", "http://ex.org/instances/{Property3/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty3/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty3/@Value\"" })}.

            ${TM_FN(1, "dataset/data1.xml", "http://ex.org/instances/{Property1/@Value}", "<http://ex.org/graph1>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty3/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty3/@Value\"" })}.
        `;
        const mapping4 = `
            ${PREFIXES}
            ${TM(0, "dataset/data3.xml", "http://ex.org/instances/{Property1/@Value}")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty1/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty1/@Value\"" })}.

            ${TM(1, "dataset/data1.xml", "http://ex.org/instances/{Property1/@Value}", "<http://ex.org/graph1>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty4/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty4/@Value\"" })}.

            ${TM(2, "dataset/data1.xml", "http://ex.org/instances/{Property1/@Value}", "<http://ex.org/graph1>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty1/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM("http://ex.org/ns/someOtherProperty", { pred: "rml:reference", obj: "\"YetAnotherProperty1/@Value\"" })}.
        `;
        await Promise.all([
            rmlStream.push(mapping1),
            rmlStream.push(mapping2),
            rmlStream.push(mapping3),
            rmlStream.push(mapping4)
        ]);
        await rmlStream.end();
    });
});

function getQuads(
    subject: OTerm,
    predicate: OTerm,
    object: OTerm,
    graph: OTerm,
    stores: Array<Store>
): Quad[] {
    const quads: Quad[] = [];

    stores.forEach(store => {
        quads.push(...store.getQuads(subject, predicate, object, graph));
    });

    return quads;
}

function getSubjects(
    predicate: OTerm,
    object: OTerm,
    graph: OTerm,
    stores: Array<Store>
): Quad_Subject[] {
    const quads: Quad_Subject[] = [];

    stores.forEach(store => {
        quads.push(...store.getSubjects(predicate, object, graph));
    });

    return quads;
}

function getObjects(
    subject: OTerm,
    predicate: OTerm,
    graph: OTerm,
    stores: Array<Store>
): Quad_Object[] {
    const quads: Quad_Object[] = [];

    stores.forEach(store => {
        quads.push(...store.getObjects(subject, predicate, graph));
    });

    return quads;
}