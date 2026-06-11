import { describe, test, expect } from "vitest";
import { FullProc } from "@rdfc/js-runner";
import { Yarrrml2RML } from "../src/yarrrml/yarrrml";
import { channel, createRunner } from "@rdfc/js-runner/lib/testUtils";
import { Parser, Store } from "n3";
import { RDF, RML, RR } from "../src/voc";
import { TEST_LOGGER as logger, readChannel } from "./utils";


describe("Functional tests for the Yarrrml2RML processor", () => {
    const yarrrmlDoc = `
        prefixes: 
            ex: "http://example.org/"
            rdfs: "http://www.w3.org/2000/01/rdf-schema#"

        mappings:
            test-mapping:
                sources:
                    - ["dataset/data.xml~xpath","/data"]
                s: ex:$(@id)
                po:
                    - [a, ex:Entity]
                    - [rdfs:label, $(@label)]
                graph: ex:myNamedGraph
    `;

    test("Given a YARRRML document it produces RML triples", async () => {
        const runner = createRunner();
        const [yarrrmlInput, reader] = channel(runner, "input");
        const [writer, rmlOutput] = channel(runner, "output");

        const rml = readChannel(rmlOutput);

        const proc = <FullProc<Yarrrml2RML>>new Yarrrml2RML(
            {
                reader,
                writer
            },
            logger
        );

        await proc.init();

        const transformPromise = proc.transform();

        await yarrrmlInput.string(yarrrmlDoc);
        await yarrrmlInput.close();
        await transformPromise;

        const store = new Store();
        store.addQuads(new Parser().parse((await rml)[0]));

        // Check that we got RML triples
        expect(store.getQuads(null, RDF.type, RML.LogicalSource, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, RDF.type, RR.TriplesMap, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, RDF.type, RR.SubjectMap, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, RDF.type, RR.PredicateObjectMap, null).length).toBeGreaterThan(0);
        expect(store.getQuads(null, RDF.type, RR.GraphMap, null).length).toBeGreaterThan(0);
    });
});