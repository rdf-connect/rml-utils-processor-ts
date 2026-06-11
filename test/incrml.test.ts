import { describe, test, expect } from "vitest";
import { FullProc } from "@rdfc/js-runner";
import { channel, createRunner } from "@rdfc/js-runner/lib/testUtils";
import { RdfStore } from "rdf-stores";
import { Parser } from "n3";
import { RDF, RR, RML, RMLT, IDLAB_FN, AS, FNML, FNO, GREL } from "../src/voc";
import { BASE, RML2IncRML, IncRMLConfig } from "../src/rml/incrml";
import { TEST_LOGGER as logger, DF, readChannel } from "./utils";


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
    const TM = (
        i: number,
        source: string,
        template: string,
        graph?: string | null | undefined,
        clazz?: string | undefined
    ) => {
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
    const TM2 = (
        i: number,
        source: string,
        template: string
    ) => {
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
    const TM_FN = (
        i: number,
        source: string,
        template: string,
        graph?: string | null | undefined,
        clazz?: string | undefined
    ) => {
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
    const POM = (
        predicate: string,
        { pred, obj }: { pred: string, obj: string }
    ) => {
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

    const POM_JOIN = (predicate: string, parentTM: string) => {
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
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>")};
            ${POM("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", { pred: "rr:constant", obj: "<http://ex.org/ns/SomeClass>" })};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })}.
        `;

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check there are 3 Triples Maps
        expect(store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null).length).toBe(3);

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(3);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null)[0]).toBeDefined();

        // Check that the watched properties template is properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value)"),
            null
        )[0]).toBeDefined();
    });

    test("1 RML mapping with 2 Triples Map without explicit target", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
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

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check there are 6 Triples Maps
        expect(store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null).length).toBe(6);

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(6);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(2);

        // Check that the watched properties template is properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value)"),
            null
        )[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.literal("prop0=Column2"), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.literal("prop1=Column3"), null)[0]).toBeDefined();
    });

    test("1 RML mapping with 2 Triples Map doing a join and without explicit target", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/someProperty", { pred: "rr:constant", obj: "\"Some Value\"" })};
            ${POM_JOIN("http://ex.org/ns/joinProperty", "http://ex.org/m1")}.

            ${TM(1, "dataset/data2.xml", "http://ex.org/instances/{Property1/@Value}")}.
        `;

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check there are 4 Triples Maps (including the join TM)
        expect(store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null).length).toBe(4);

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(3);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null)[0]).toBeDefined();

        // Check that the watched properties template is properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value)"),
            null
        )[0]).toBeDefined();
    });

    test("1 RML mapping with 2 Triples Maps (same source, same template, same named graph) with versioned IRIs", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

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

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })}.

            ${TM(1, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{YetAnotherProperty/@Value}\"" })}.
        `;

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check that Logical Target triples are defined
        expect(store.getQuads(DF.namedNode(`${BASE}LDES_LT`), null, null, null).length).toBe(5);

        // Check there are 3 Triples Maps
        const tms = store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(3);

        // Check that each associated Subject Map is linked to the proper Logical Target
        tms.forEach(tm => {
            const smQ = store.getQuads(tm.subject, RR.terms.subjectMap, null, null)[0];
            expect(store.getQuads(smQ.object, RML.terms.logicalTarget, DF.namedNode(`${BASE}LDES_LT`), null)[0]).toBeDefined();
        });

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(3);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null)[0]).toBeDefined();
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null)[0]).toBeDefined();

        // Check that the watched properties template is properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || YetAnotherProperty/@Value)"),
            null
        )[0]).toBeDefined();
    });

    test("1 RML mapping with 2 Triples Maps (same source, same template, different named graph) with versioned IRIs", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

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

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
        // Push a mapping
        const mapping = `
            ${PREFIXES}
            ${TM(0, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph1>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{AnotherProperty/@Value}\"" })}.

            ${TM(1, "dataset/data.xml", "http://ex.org/instances/{Property/@Value}", "<http://ex.org/myGraph2>", "<http://ex.org/ns/SomeClass>")};
            ${POM("http://ex.org/ns/type", { pred: "rr:template", obj: "\"http://ex.org/instance/{YetAnotherProperty/@Value}\"" })};
            ${POM("http://ex.org/ns/type", { pred: "rml:reference", obj: "\"@Name\"" })}.
        `;

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check that Logical Target triples are defined
        expect(store.getQuads(DF.namedNode(`${BASE}LDES_LT`), null, null, null).length).toBe(5);

        // Check there are 6 Triples Maps
        const tms = store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(6);

        // Check that each associated Subject Map is linked to the proper Logical Target
        tms.forEach(tm => {
            const smQ = store.getQuads(tm.subject, RR.terms.subjectMap, null, null)[0];
            expect(store.getQuads(smQ.object, RML.terms.logicalTarget, DF.namedNode(`${BASE}LDES_LT`), null)[0]).toBeDefined();
        });

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(6);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(2);

        // Check that states are different for all Triples Maps
        const statePOMs = store.getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
        const states: string[] = [];
        statePOMs.forEach(pom => {
            const om = store.getQuads(pom.subject, RR.terms.objectMap, null)[0];
            states.push(store.getQuads(om.object, RR.terms.constant, null)[0].object.value);
        });
        expect(new Set(states).size).toBe(states.length);

        // Check that the watched properties templates are properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value)"),
            null
        )[0]).toBeDefined();
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || YetAnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
            null
        )[0]).toBeDefined();
    });

    test("1 RML mapping with 3 Triples Maps (same source, different template, no named graph) without explicit target", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
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

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check there are 9 Triples Maps
        const tms = store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(9);

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(9);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(3);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(3);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(3);

        // Check that states are different for all Triples Maps
        const statePOMs = store.getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
        const states: string[] = [];
        statePOMs.forEach(pom => {
            const om = store.getQuads(pom.subject, RR.terms.objectMap, null, null)[0].object;
            states.push(store.getQuads(om, RR.terms.constant, null, null)[0].object.value);
        });
        expect(new Set(states).size).toBe(states.length);

        // Check that the watched properties templates are properly defined
        expect(store.getQuads(null, RML.terms.reference, DF.literal(""), null).length).toBe(3);
    });

    test("1 RML mapping with 2 Triples Maps (different source, same template, no named graph) with versioned IRIs", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

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

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
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

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check that Logical Target triples are defined
        expect(store.getQuads(DF.namedNode(`${BASE}LDES_LT`), null, null, null).length).toBe(5);

        // Check there are 6 Triples Maps
        const tms = store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(6);

        // Check that each associated Subject Map is linked to the proper Logical Target
        tms.forEach(tm => {
            const smQ = store.getQuads(tm.subject, RR.terms.subjectMap, null, null)[0];
            expect(store.getQuads(smQ.object, RML.terms.logicalTarget, DF.namedNode(`${BASE}LDES_LT`), null)[0]).toBeDefined();
        });

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(6);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(2);

        // Check that states are different for all Triples Maps
        const statePOMs = store.getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
        const states: string[] = [];
        statePOMs.forEach(pom => {
            const om = store.getQuads(pom.subject, RR.terms.objectMap, null, null)[0].object;
            states.push(store.getQuads(om, RR.terms.constant, null, null)[0].object.value);
        });
        expect(new Set(states).size).toBe(states.length);

        // Check that the watched properties templates are properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || SomeProperty/@Name)"),
            null
        )[0]).toBeDefined();
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
            null
        )[0]).toBeDefined();
    });

    test("1 RML mapping with 2 (Fn) Triples Maps (same source, different template, no named graph) without explicit target", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
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

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check there are 6 Triples Maps
        const tms = store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(6);

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(6);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(2);

        // Check that states are different for all Triples Maps
        const statePOMs = store.getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
        const states: string[] = [];
        statePOMs.forEach(pom => {
            const om = store.getQuads(pom.subject, RR.terms.objectMap, null, null)[0].object;
            states.push(store.getQuads(om, RR.terms.constant, null, null)[0].object.value);
        });
        expect(new Set(states).size).toBe(states.length);

        // Check that the watched properties templates are properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || SomeProperty/@Name)"),
            null
        )[0]).toBeDefined();
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || YetAnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
            null
        )[0]).toBeDefined();

        // Check that conditional function is embedded correctly on stateful mappings
        tms.forEach(tm => {
            const sm = store.getQuads(tm.subject, RR.terms.subjectMap, null)[0]?.object;
            if (sm) {
                const fv = store.getQuads(sm, FNML.terms.functionValue, null)[0]?.object;
                if (fv) {
                    const poms = store.getQuads(fv, RR.terms.predicateObjectMap, null);
                    expect(poms.length).toBeGreaterThanOrEqual(3);
                    // Check the IRI template param is the result of an embedded function
                    const iriPom = poms.find(pom => {
                        const iriPm = store.getQuads(pom.object, RR.terms.predicate, null)[0];
                        return iriPm && iriPm.object.value === IDLAB_FN.iri;
                    });
                    if (iriPom) {
                        const iriOm = store.getQuads(iriPom.object, RR.terms.objectMap, null)[0]?.object;
                        if (iriOm) {
                            const iriFnTm = store.getQuads(iriOm, FNML.terms.functionValue, null)[0]?.object;
                            if (iriFnTm) {
                                const iriFnPoms = store.getQuads(iriFnTm, RR.terms.predicateObjectMap, null);
                                expect(iriFnPoms.some(pom => {
                                    const exec = store.getQuads(pom.object, RR.terms.predicate, null)[0];
                                    const fnObj = store.getQuads(pom.object, RR.terms.objectMap, null)[0];
                                    const fn = fnObj ? store.getQuads(fnObj.object, RR.terms.constant, null)[0] : null;
                                    if (fn && exec) {
                                        return exec.object.value === FNO.executes && fn.object.value === IDLAB_FN.trueCondition;
                                    }
                                })).toBeTruthy();
                            }
                        }
                    }
                }
            }
        });
    });

    test("1 RML mapping with 2 (Fn) Triples Maps (different source, different template, same named graph) with versioned IRIs", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

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

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
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

        await rmlInput.string(mapping);
        await rmlInput.close();
        await transformPromise;

        const store = RdfStore.createDefault();
        new Parser().parse((await incrml)[0]).forEach(quad => store.addQuad(quad as any));

        // Check that Logical Target triples are defined
        expect(store.getQuads(DF.namedNode(`${BASE}LDES_LT`), null, null, null).length).toBe(5);

        // Check there are 6 Triples Maps
        const tms = store.getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(6);

        // Check that each associated Subject Map is linked to the proper Logical Target
        tms.forEach(tm => {
            const smQ = store.getQuads(tm.subject, RR.terms.subjectMap, null, null)[0];
            expect(store.getQuads(smQ.object, RML.terms.logicalTarget, DF.namedNode(`${BASE}LDES_LT`), null)[0]).toBeDefined();
        });

        // Check there are Object Maps pointing to lifecycle entities
        expect(store.getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(6);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(2);
        expect(store.getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(2);

        // Check that states are different for all Triples Maps
        const statePOMs = store.getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
        const states: string[] = [];
        statePOMs.forEach(pom => {
            const om = store.getQuads(pom.subject, RR.terms.objectMap, null, null)[0].object;
            states.push(store.getQuads(om, RR.terms.constant, null, null)[0].object.value);
        });
        expect(new Set(states).size).toBe(states.length);

        // Check that the watched properties templates are properly defined
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty/@Value || '&' || 'prop1=' || SomeProperty/@Name)"),
            null
        )[0]).toBeDefined();
        expect(store.getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || YetAnotherProperty/@Value || '&' || 'prop1=' || @Name)"),
            null
        )[0]).toBeDefined();

        // Check that conditional function is embedded correctly on stateful mappings
        tms.forEach(tm => {
            const sm = store.getQuads(tm.subject, RR.terms.subjectMap, null)[0]?.object;
            if (sm) {
                const fv = store.getQuads(sm, FNML.terms.functionValue, null)[0]?.object;
                if (fv) {
                    const poms = store.getQuads(fv, RR.terms.predicateObjectMap, null);
                    expect(poms.length).toBeGreaterThanOrEqual(3);
                    // Check the IRI template param is the result of an embedded function
                    const iriPom = poms.find(pom => {
                        const iriPm = store.getQuads(pom.object, RR.terms.predicate, null)[0];
                        return iriPm && iriPm.object.value === IDLAB_FN.iri;
                    });
                    if (iriPom) {
                        const iriOm = store.getQuads(iriPom.object, RR.terms.objectMap, null)[0]?.object;
                        if (iriOm) {
                            const iriFnTm = store.getQuads(iriOm, FNML.terms.functionValue, null)[0]?.object;
                            if (iriFnTm) {
                                const iriFnPoms = store.getQuads(iriFnTm, RR.terms.predicateObjectMap, null);
                                expect(iriFnPoms.some(pom => {
                                    const exec = store.getQuads(pom.object, RR.terms.predicate, null)[0];
                                    const fnObj = store.getQuads(pom.object, RR.terms.objectMap, null)[0];
                                    const fn = fnObj ? store.getQuads(fnObj.object, RR.terms.constant, null)[0] : null;
                                    if (fn && exec) {
                                        return exec.object.value === FNO.executes && fn.object.value === IDLAB_FN.trueCondition;
                                    }
                                })).toBeTruthy();
                            }
                        }
                    }
                }
            }
        });
    });

    test("2 RML mappings with 2 Triples Map (different source, different template, no named graph) without explicit target", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

        const config: IncRMLConfig = {
            stateBasePath: ".",
            lifeCycleConfig: {
                predicate: "http://ex.org/ns/lifeCycleProperty",
                create: { function: IDLAB_FN.explicitCreate, type: AS.Create },
                update: { function: IDLAB_FN.implicitUpdate, type: AS.Update },
                delete: { function: IDLAB_FN.implicitDelete, type: AS.Delete }
            }
        };

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
        // Send mappings
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

        await rmlInput.string(mapping1);
        await rmlInput.string(mapping2);
        await rmlInput.close();
        await transformPromise;

        const stores = (await incrml).map(str => {
            const s = RdfStore.createDefault();
            new Parser().parse(str).forEach(quad => s.addQuad(quad as any));
            return s;
        });
        const getQuads = (subject: any, predicate: any, object: any, graph: any = null) => {
            const quads: any[] = [];
            stores.forEach(s => quads.push(...s.getQuads(subject, predicate, object, graph)));
            return quads;
        };

        // Check there are 6 Triples Maps
        const tms = getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(6);

        // Check there are Object Maps pointing to lifecycle entities
        expect(getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(6);
        expect(getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(2);
        expect(getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(2);
        expect(getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(2);

        // Check that states are different for all Triples Maps
        const states: string[] = [];
        stores.forEach(s => {
            const statePOMs = s.getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
            statePOMs.forEach(pom => {
                const om = s.getQuads(pom.subject, RR.terms.objectMap, null, null)[0].object;
                states.push(s.getQuads(om, RR.terms.constant, null, null)[0].object.value);
            });
        });
        expect(new Set(states).size).toBe(states.length);

        // Check that the watched properties templates are properly defined
        expect(getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty1/@Value || '&' || 'prop1=' || YetAnotherProperty1/@Value)"),
            null
        )[0]).toBeDefined();
        expect(getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty2/@Value || '&' || 'prop1=' || YetAnotherProperty2/@Value)"),
            null
        )[0]).toBeDefined();
    });

    test("4 RML mapping with 8 (Fn) Triples Maps (different/same source, different/same template, different/same named graph) with versioned IRIs", async () => {
        const runner = createRunner();
        const [rmlInput, reader] = channel(runner, "rml");
        const [writer, incrmlOutput] = channel(runner, "incrml");

        const incrml = readChannel(incrmlOutput);

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

        const proc = <FullProc<RML2IncRML>>new RML2IncRML({
            rmlStream: reader,
            incrmlStream: writer,
            config,
        }, logger);

        await proc.init();

        const transformPromise = proc.transform();
        // Send mappings
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

        await rmlInput.string(mapping1);
        await rmlInput.string(mapping2);
        await rmlInput.string(mapping3);
        await rmlInput.string(mapping4);
        await rmlInput.close();
        await transformPromise;

        const stores = (await incrml).map(str => {
            const s = RdfStore.createDefault();
            new Parser().parse(str).forEach(quad => s.addQuad(quad as any));
            return s;
        });
        const getQuads = (subject: any, predicate: any, object: any, graph: any = null) => {
            const quads: any[] = [];
            stores.forEach(s => quads.push(...s.getQuads(subject, predicate, object, graph)));
            return quads;
        };

        // Check that Logical Target triples are defined
        expect(getQuads(DF.namedNode(`${BASE}LDES_LT`), null, null, null).length).toBe(20);

        // Check there are 21 Triples Maps
        const tms = getQuads(null, RDF.terms.type, RR.terms.TriplesMap, null);
        expect(tms.length).toBe(21);

        // Check that each associated Subject Map is linked to the proper Logical Target
        tms.forEach(tm => {
            const smQ = getQuads(tm.subject, RR.terms.subjectMap, null, null)[0];
            expect(getQuads(smQ.object, RML.terms.logicalTarget, DF.namedNode(`${BASE}LDES_LT`), null)[0]).toBeDefined();
        });

        // Check there are Object Maps pointing to lifecycle entities
        expect(getQuads(null, RR.terms.predicate, DF.namedNode(config.lifeCycleConfig.predicate), null).length).toBe(21);
        expect(getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.create.type), null).length).toBe(7);
        expect(getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.update.type), null).length).toBe(7);
        expect(getQuads(null, RR.terms.constant, DF.namedNode(config.lifeCycleConfig.delete.type), null).length).toBe(7);

        // Check that states are different for all Triples Maps
        const statePOMs = getQuads(null, RR.terms.predicate, IDLAB_FN.terms.state, null);
        const stateSet = new Set<string>();
        statePOMs.forEach(pom => {
            const oms = getQuads(pom.subject, RR.terms.objectMap, null);
            oms.forEach(om => {
                getQuads(om.object, RR.terms.constant, null).forEach(state => stateSet.add(state.object.value));
            });
        });
        expect(stateSet.size).toBe(18);

        // Check that the watched properties templates are properly defined
        expect(getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty1/@Value || '&' || 'prop1=' || YetAnotherProperty1/@Value)"),
            null
        )[0]).toBeDefined();
        expect(getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty2/@Value || '&' || 'prop1=' || YetAnotherProperty2/@Value)"),
            null
        )[0]).toBeDefined();
        expect(getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty2.5/@Value || '&' || 'prop1=' || YetAnotherProperty2.5/@Value)"),
            null
        )[0]).toBeDefined();
        const wp3 = getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty3/@Value || '&' || 'prop1=' || YetAnotherProperty3/@Value)"),
            null
        );
        expect(wp3.length).toBe(2);
        expect(getQuads(
            null,
            RML.terms.reference,
            DF.literal("('prop0=' || AnotherProperty4/@Value || '&' || 'prop1=' || YetAnotherProperty4/@Value || '&' || 'prop2=' || AnotherProperty1/@Value || '&' || 'prop3=' || YetAnotherProperty1/@Value)"),
            null
        )[0]).toBeDefined();

        // Check that conditional function is embedded correctly on stateful mappings
        tms.forEach(tm => {
            const sm = getQuads(tm.subject, RR.terms.subjectMap, null)[0]?.object;
            if (sm) {
                const fv = getQuads(sm, FNML.terms.functionValue, null)[0]?.object;
                if (fv) {
                    const poms = getQuads(fv, RR.terms.predicateObjectMap, null);
                    expect(poms.length).toBeGreaterThanOrEqual(3);
                    // Check the IRI template param is the result of an embedded function
                    const iriPom = poms.find(pom => {
                        const iriPm = getQuads(pom.object, RR.terms.predicate, null)[0];
                        return iriPm && iriPm.object.value === IDLAB_FN.iri;
                    });
                    if (iriPom) {
                        const iriOm = getQuads(iriPom.object, RR.terms.objectMap, null)[0]?.object;
                        if (iriOm) {
                            const iriFnTm = getQuads(iriOm, FNML.terms.functionValue, null)[0]?.object;
                            if (iriFnTm) {
                                const iriFnPoms = getQuads(iriFnTm, RR.terms.predicateObjectMap, null);
                                expect(iriFnPoms.some(pom => {
                                    const exec = getQuads(pom.object, RR.terms.predicate, null)[0];
                                    const fnObj = getQuads(pom.object, RR.terms.objectMap, null)[0];
                                    const fn = fnObj ? getQuads(fnObj.object, RR.terms.constant, null)[0] : null;
                                    if (fn && exec) {
                                        return exec.object.value === FNO.executes && fn.object.value === IDLAB_FN.trueCondition;
                                    }
                                })).toBeTruthy();
                            }
                        }
                    }
                }
            }
        });
    });
});