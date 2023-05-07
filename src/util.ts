import { existsSync, createReadStream } from "fs";
import { randomUUID } from "crypto";
import { exec } from "child_process";
import { Store, StreamParser } from "n3";
import * as N3 from "n3";

import stream from "stream";
import http from "http";
import https from "https";
import { createUriAndTermNamespace } from "@treecg/types";
import { Term } from "@rdfjs/types";

const defaultLocation = "/tmp/rml-" + randomUUID() + ".jar";
let rmlJarPromise: undefined | Promise<string> = undefined;

const { namedNode } = N3.DataFactory;
const OWL = createUriAndTermNamespace("http://www.w3.org/2002/07/owl#", "imports");

export function recursiveDelete(subject: Term, store: Store) {
  for (let q of store.getQuads(subject, null, null, null)) {
    store.delete(q);
    recursiveDelete(q.object, store);
  }
}

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

async function get_readstream(location: string): Promise<stream.Readable> {
  if (location.startsWith("https")) {
    return new Promise((res) => {
      https.get(location, res);
    });
  } else if (location.startsWith("http")) {
    return new Promise((res) => {
      http.get(location, res);
    });
  } else {
    return createReadStream(location);
  }
}

const loaded = new Set();
async function _load_store(location: string, store: Store, recursive: boolean) {
  if (loaded.has(location)) { return; }
  loaded.add(location);

  console.log("Loading", location);

  try {
    const parser = new StreamParser({ baseIRI: location });
    const rdfStream = await get_readstream(location);
    rdfStream.pipe(parser);

    await new Promise((res, rej) => {
      const ev = store.import(parser);
      ev.on('end', res); ev.on("error", rej)
    });

    if (recursive) {
      const other_imports = store.getObjects(namedNode(location), OWL.terms.imports, null)
      for (let other of other_imports) {
        await _load_store(other.value, store, true);
      }
    }
  } catch (ex: any) {
    console.error("Loading Failed");
    console.error(ex)
  }
}
export async function load_store(location: string, store: Store, recursive = true) {
  const out = await _load_store(location, store, recursive);
  loaded.clear();
  return out;
}
