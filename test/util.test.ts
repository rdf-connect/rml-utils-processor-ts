import { describe, expect, test, vi } from "vitest";
import { EventEmitter } from "events";

const { execFile } = vi.hoisted(() => ({ execFile: vi.fn() }));
vi.mock("child_process", async (original) => ({
    ...(await original<typeof import("child_process")>()),
    execFile,
}));

import { getJarFile } from "../src/util";

describe("getJarFile", () => {
    test("downloads to a location with spaces as one argument, without a shell", async () => {
        execFile.mockImplementation(() => {
            const proc = new EventEmitter();
            setImmediate(() => proc.emit("exit", 0));
            return proc;
        });

        const location = await getJarFile("file:///tmp/rml%20test%20dir/rmlmapper.jar");

        expect(location).toBe("/tmp/rml test dir/rmlmapper.jar");
        expect(execFile).toHaveBeenCalledWith("wget", [
            expect.stringMatching(/^https:\/\/.*\.jar$/),
            "-O",
            "/tmp/rml test dir/rmlmapper.jar",
        ]);
    });
});
