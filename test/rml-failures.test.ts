import { beforeEach, describe, expect, test, vi } from "vitest";
import { EventEmitter } from "events";
import { FullProc } from "@rdfc/js-runner";
import { channel, createRunner } from "@rdfc/js-runner/lib/testUtils";

// java exits with the next queued exit code, anything else (wget) succeeds
const { spawn, javaExitCodes } = vi.hoisted(() => ({
    spawn: vi.fn(),
    javaExitCodes: [] as number[],
}));
vi.mock("child_process", async (original) => ({
    ...(await original<typeof import("child_process")>()),
    spawn,
}));

import { RMLMapperJS, Source } from "../src/rml/rml";
import { TEST_LOGGER as logger } from "./utils";

const MAPPING = `
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix rml: <http://semweb.mmlab.be/ns/rml#> .
    @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
    @prefix ex: <http://example.org/> .

    ex:map a rr:TriplesMap ;
        rml:logicalSource [
            a rml:LogicalSource ;
            rml:source "dataset/data.xml" ;
            rml:iterator "//data" ;
            rml:referenceFormulation ql:XPath
        ] ;
        rr:subjectMap [ rr:template "http://example.org/{@id}" ] .
`;

function fakeProc(code: number) {
    const proc = Object.assign(new EventEmitter(), {
        stdout: new EventEmitter(),
        stderr: new EventEmitter(),
    });
    setImmediate(() => proc.emit("close", code, null));
    return proc;
}

function createMapper(trigger: boolean) {
    const runner = createRunner();
    const [rmlInput, mappingReader] = channel(runner, "rml");
    const [defaultOutputStream] = channel(runner, "defaultOutput");
    const [sourceInput, sourceInputStream] = channel(runner, "dataInput");
    const sources: Source[] = [
        {
            location: "dataset/data.xml",
            dataInput: sourceInputStream,
            hasData: false,
            trigger
        }
    ];
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
    return { proc, rmlInput, sourceInput };
}

const javaRuns = () => spawn.mock.calls.filter(([command]) => command === "java").length;

describe("RMLMapperJS when RMLMapper fails", () => {
    beforeEach(() => {
        javaExitCodes.length = 0;
        spawn.mockReset();
        spawn.mockImplementation((command: string) =>
            fakeProc(command === "java" ? javaExitCodes.shift() ?? 0 : 0));
    });

    test("fails when the run started by the end of the mappings fails, while the source stays open", async () => {
        javaExitCodes.push(1);
        const { proc, rmlInput, sourceInput } = createMapper(false);

        await proc.init();
        const transformPromise = proc.transform();

        await sourceInput.string(`<data id="1"/>`);
        await rmlInput.string(MAPPING);
        await rmlInput.close();

        // The source is never closed
        await expect(transformPromise).rejects.toThrow("RMLMapper exited with code 1");
        expect(javaRuns()).toBe(1);
    });

    test("fails when a run started by a source update fails, while the source stays open", async () => {
        javaExitCodes.push(0, 1);
        const { proc, rmlInput, sourceInput } = createMapper(true);

        await proc.init();
        const transformPromise = proc.transform();

        await sourceInput.string(`<data id="1"/>`);
        await rmlInput.string(MAPPING);
        await rmlInput.close();
        // Not awaited: the failed run ends the reader, so it never asks for the next message
        void sourceInput.string(`<data id="2"/>`);

        // The source is never closed
        await expect(transformPromise).rejects.toThrow("RMLMapper exited with code 1");
        expect(javaRuns()).toBe(2);
    });
});
