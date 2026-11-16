import { describe, expect, test, vi } from "vitest";
import { EventEmitter } from "events";

const { spawn } = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("child_process", async (original) => ({
  ...(await original<typeof import("child_process")>()),
  spawn,
}));

import { getJarFile } from "../src/util";

describe("getJarFile", () => {
  test("downloads to a location with spaces as one argument, without a shell", async () => {
    spawn.mockImplementation(() => {
      const proc = new EventEmitter();
      setImmediate(() => proc.emit("close", 0));
      return proc;
    });

    const location = await getJarFile(
      "file:///tmp/rml%20test%20dir/rmlmapper.jar",
    );

    expect(location).toBe("/tmp/rml test dir/rmlmapper.jar");
    expect(spawn).toHaveBeenCalledWith(
      "wget",
      [
        expect.stringMatching(/^https:\/\/.*\.jar$/),
        "-O",
        "/tmp/rml test dir/rmlmapper.jar",
      ],
      { stdio: "ignore" },
    );
  });
});
