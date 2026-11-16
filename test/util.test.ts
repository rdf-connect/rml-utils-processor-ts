import { describe, expect, test, vi } from "vitest";
import { EventEmitter } from "events";
import { existsSync, writeFileSync } from "fs";
import { mkdtemp, rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";

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

  test("removes what wget left behind when the download fails", async () => {
    const dir = await mkdtemp(join(tmpdir(), "rml-jar-"));
    const location = join(dir, "rmlmapper.jar");
    spawn.mockImplementation(() => {
      // Like wget -O, create the file before failing
      writeFileSync(location, "");
      const proc = new EventEmitter();
      setImmediate(() => proc.emit("close", 8, null));
      return proc;
    });

    try {
      // A fresh module, as the download of the test above is cached
      vi.resetModules();
      const { getJarFile } = await import("../src/util");

      await expect(getJarFile(location)).rejects.toThrow("wget exited with code 8");
      expect(existsSync(location)).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
