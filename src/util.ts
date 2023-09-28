import { existsSync, createReadStream } from "fs";
import { randomUUID } from "crypto";
import { exec } from "child_process";
import * as N3 from "n3";


import { createUriAndTermNamespace } from "@treecg/types";

const defaultLocation = "/tmp/rml-" + randomUUID() + ".jar";
let rmlJarPromise: undefined | Promise<string> = undefined;

const { namedNode } = N3.DataFactory;
const OWL = createUriAndTermNamespace("http://www.w3.org/2002/07/owl#", "imports");


export async function getJarFile(mLocation: string | undefined, offline: boolean, url: string): Promise<string> {
  const location = mLocation || defaultLocation;

  try {
    if (existsSync(location)) {
      return location;
    }
  } catch (e: any) { }

  // Did not find the file :/
  if (offline) {
    throw "Did not find jar file, and the runner is started in offline mode. Cannot continue.";
  }

  if (!rmlJarPromise) {
    rmlJarPromise = (async function() {
      const cmd = `wget ${url} -O ${location}`;
      console.log("Executing $", cmd)
      const proc = exec(cmd);
      await new Promise(res => proc.once("exit", res));
      return location;
    })();
  }

  return rmlJarPromise;
}
