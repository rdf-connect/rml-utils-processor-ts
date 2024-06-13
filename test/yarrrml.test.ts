import { describe, test, expect } from "@jest/globals";
import { SimpleStream } from "@rdfc/js-runner";
import { yarrrml2rml } from "../src/yarrrml/yarrrml";
import { Parser, Store } from "n3";
import { RDF, RML, RR } from "../src/voc";

describe("Functional tests for the yarrrml2rml Connector Architecture function", () => {
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
        const reader = new SimpleStream<string>();
        const writer = new SimpleStream<string>();

        writer.data((data: string) => {
            const store = new Store();
            store.addQuads(new Parser().parse(data));

            // Check that we got RML triples
            expect(store.getQuads(null, RDF.type, RML.LogicalSource, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, RDF.type, RR.TriplesMap, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, RDF.type, RR.SubjectMap, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, RDF.type, RR.PredicateObjectMap, null).length).toBeGreaterThan(0);
            expect(store.getQuads(null, RDF.type, RR.GraphMap, null).length).toBeGreaterThan(0);
        });

        // Execute function
        yarrrml2rml(reader, writer);

        // Push some data to the input stream
        await reader.push(yarrrmlDoc);
        await reader.end();
    });
});