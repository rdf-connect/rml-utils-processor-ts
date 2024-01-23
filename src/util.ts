import { existsSync } from "fs";
import { randomUUID as cryptoUUID } from "crypto";
import { exec } from "child_process";

const DEFAULT_URL = "https://github.com/RMLio/rmlmapper-java/releases/download/v6.5.1/rmlmapper-6.5.1-r371-all.jar";
const DEFAULT_LOCATION = "/tmp/rml-" + cryptoUUID() + ".jar";

let rmlJarPromise: undefined | Promise<string> = undefined;

export async function getJarFile(mLocation: string | undefined): Promise<string> {
  let location;
  let url;

  if (mLocation) {
    if (mLocation.startsWith("http")) {
      url = mLocation;
      location = `./${mLocation.split("/")[mLocation.split("/").length - 1]}`;
    } else {
      location = mLocation;
      url = DEFAULT_URL;
    }
  } else {
    location = DEFAULT_LOCATION;
    url = DEFAULT_URL;
  }

  try {
    if (existsSync(location)) {
      return location;
    }
  } catch (e: any) {
    throw new Error(`Did not find given jar file ${location}`);
  }

  if (!rmlJarPromise) {
    rmlJarPromise = (async function () {
      const cmd = `wget ${url} -O ${location}`;
      console.log("Executing $", cmd)
      const proc = exec(cmd);
      await new Promise(res => proc.once("exit", res));
      return location;
    })();
  }

  return rmlJarPromise;
}

export function randomUUID(length = 8) {
  // declare all characters
  const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }

  return result;
}
