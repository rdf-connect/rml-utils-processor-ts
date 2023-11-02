import { describe, expect, test } from "@jest/globals";
import { extractProcessors, extractSteps, Source } from "@ajuvercr/js-runner";
import { resolve } from "path";

describe("Tests for RML-related processors", async () => {
    const pipeline = `
        @prefix js: <https://w3id.org/conn/js#>.
        @prefix ws: <https://w3id.org/conn/ws#>.
        @prefix : <https://w3id.org/conn#>.
        @prefix owl: <http://www.w3.org/2002/07/owl#>.
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
        @prefix sh: <http://www.w3.org/ns/shacl#>.

        <> owl:imports <./node_modules/@ajuvercr/js-runner/ontology.ttl>, <./processors.ttl>.

        [ ] a :Channel;
            :reader <jr>;
            :writer <jw>.
        <jr> a js:JsReaderChannel.
        <jw> a js:JsWriterChannel.
    `;

    const baseIRI = process.cwd() + "/config.ttl";

    test("js:Y2R is properly defined", async () => {
        const proc = `
        [ ] a js:Y2R; 
            js:input <jr>; 
            js:output <jw>.`;

        const source: Source = {
            value: pipeline + proc,
            baseIRI,
            type: "memory",
        };

        const { processors, quads, shapes: config } = await extractProcessors(source);

        const env = processors.find((x) => x.ty.value === "https://w3id.org/conn/js#Y2R")!;
        expect(env).toBeDefined();

        const argss = extractSteps(env, quads, config);
        expect(argss.length).toBe(1);
        expect(argss[0].length).toBe(2);

        const [[input, output]] = argss;
        testReader(input);
        testWriter(output);

        await checkProc(env.file, env.func);
    });

    test("js:RMLMapperReader is properly defined", async () => {
        const proc = `
            [ ] a js:RMLMapperReader; 
                js:rmlSource [
                    js:sourceLocation "dataset/data.xml";
                    js:input <jr>;
                    js:trigger true
                ];
                js:rmlTarget [
                    js:targetLocation "dataset/output.nt";
                    js:output <jw>
                ];
                js:mappings <jr>;
                js:output <jw>;
                js:rmlJar <./rmlmapper-6.3.0-r0-all.jar>.
        `;

        const source: Source = {
            value: pipeline + proc,
            baseIRI,
            type: "memory",
        };

        const { processors, quads, shapes: config } = await extractProcessors(source);

        const env = processors.find((x) => x.ty.value === "https://w3id.org/conn/js#RMLMapperReader")!;
        expect(env).toBeDefined();

        const argss = extractSteps(env, quads, config);
        expect(argss.length).toBe(1);
        expect(argss[0].length).toBe(5);

        const [[mappings, rmlSource, rmlTarget, output, rmlJar]] = argss;
        
        expect(rmlSource[0].location).toBe("dataset/data.xml");
        testReader(rmlSource[0].dataInput);
        expect(rmlSource[0].trigger).toBeTruthy();
        
        expect(rmlTarget[0].location).toBe("dataset/output.nt");
        testWriter(rmlTarget[0].writer);

        testReader(mappings);
        testWriter(output);
        expect(rmlJar).toBe(resolve("./rmlmapper-6.3.0-r0-all.jar"));

        await checkProc(env.file, env.func);
    });
});

function testReader(arg: any) {
    expect(arg).toBeInstanceOf(Object);
    expect(arg.channel).toBeDefined();
    expect(arg.channel.id).toBeDefined();
    expect(arg.ty).toBeDefined();
}

function testWriter(arg: any) {
    expect(arg).toBeInstanceOf(Object);
    expect(arg.channel).toBeDefined();
    expect(arg.channel.id).toBeDefined();
    expect(arg.ty).toBeDefined();
}

async function checkProc(location: string, func: string) {
    const mod = await import("file://" + location);
    expect(mod[func]).toBeDefined();
}