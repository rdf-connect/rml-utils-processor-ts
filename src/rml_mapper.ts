import type { Stream, Writer } from "@ajuvercr/js-runner";
import { exec } from "child_process";
import { readFile, writeFile } from "fs/promises";
import {
  DataFactory,
  Parser,
  Quad_Predicate,
  Quad_Subject,
  Store,
  Writer as N3Writer,
} from "n3";

import { getJarFile } from "./util";
import { Cont, empty, match, pred, subject } from "rdf-lens";
import { RDF } from "@treecg/types";
import { RML } from "./voc";

// declare all characters
const characters =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

function randomUUID(length = 8) {
  let result = "";
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }

  return result;
}

const { literal } = DataFactory;
const RML_MAPPER_RELEASE =
  "https://github.com/RMLio/rmlmapper-java/releases/download/v6.2.0/rmlmapper-6.2.0-r368-all.jar";

const transformMapping = (input: string, sources: Source[]) => {
  const quads = new Parser().parse(input);
  const store = new Store(quads);
  // Extract logical Sources from incoming mapping
  const extractSource = empty<Cont>()
    .map(({ id }) => ({ subject: id }))
    .and(
      pred(RML.terms.source)
        .one()
        .map(({ id }) => ({ source: id.value })),
    )
    .map(([{ subject }, { source }]) => ({ subject, source }));
  const sourcesLens = match(
    undefined,
    RDF.terms.type,
    RML.terms.custom("LogicalSource"),
  )
    .thenAll(subject)
    .thenAll(extractSource); // Logical sources
  const foundSources = sourcesLens.execute(quads);
  console.log("Found sources", foundSources);

  // There exists a source that has no defined source, we cannot map this mapping
  for (let foundSource of foundSources) {
    let found = false;
    for (let source of sources) {
      if (source.location === foundSource.source) {
        console.log(
          "Moving location",
          foundSource.source,
          "to",
          source.newLocation,
        );
        found = true;
        // Remove the old location
        store.removeQuad(
          <Quad_Subject>foundSource.subject,
          <Quad_Predicate>RML.terms.source,
          literal(foundSource.source),
        );

        // Add the new location
        store.addQuad(
          <Quad_Subject>foundSource.subject,
          <Quad_Predicate>RML.terms.source,
          literal(source.newLocation),
        );
        break;
      }
    }
    if (!found) {
      throw `Logical source ${foundSource.subject.value} has no configured source (${foundSource.source}) channel!`;
    }
  }

  return new N3Writer().quadsToString(store.getQuads(null, null, null, null));
};

export type Source = {
  location: string;
  dataInput: Stream<string>;
  newLocation: string;
  hasData: boolean;
};

export type Target = {
  location: string;
  writer: Writer<string>;
};

export async function newMapper(
  sources: Source[],
  mappingReader: Stream<string>,
  defaultWriter: Writer<string>,
  appendMapping?: boolean,
  jarLocation?: string,
) {
  const uid = randomUUID();
  const outputFile = "/tmp/rml-" + uid + "-output.ttl";

  for (let source of sources) {
    const filename = source.location.split("/").pop();
    source.newLocation = `/tmp/rml-${uid}-input-${randomUUID()}-${filename}`;
    source.hasData = false;
  }

  const jarFile = await getJarFile(jarLocation, false, RML_MAPPER_RELEASE);
  const mappingLocations: string[] = [];

  const map = async () => {
    let out = "";
    for (let mappingFile of mappingLocations) {
      console.log("Running", mappingFile);
      const command = `java -jar ${jarFile} -m ${mappingFile} -o ${outputFile}`;

      const proc = exec(command);
      proc.stdout!.on("data", function (data) {
        console.log("rml mapper std: ", data.toString());
      });
      proc.stderr!.on("data", function (data) {
        console.error("rml mapper err:", data.toString());
      });
      await new Promise((res) => proc.on("exit", res));

      out += await readFile(outputFile, { encoding: "utf8" });
      console.log("Done", mappingFile);
    }

    console.log("All done");
    await defaultWriter.push(out);

  };

  mappingReader.data(async (input) => {
    console.log("Got mapping input!");
    try {
      const newMapping = transformMapping(input, sources);
      if (mappingLocations.length < 1 || appendMapping) {
        const newLocation = `/tmp/rml-${uid}-mapping-${mappingLocations.length}.ttl`;
        await writeFile(newLocation, newMapping, { encoding: "utf8" });
        mappingLocations.push(newLocation);
        console.log("Add new mapping file", newLocation);
      } else {
        await writeFile(mappingLocations[0], newMapping, { encoding: "utf8" });
        console.log("Overwriting mapping file at", mappingLocations[0]);
      }
    } catch (ex) {
      console.error("Could not map incoming rml input");
      console.error(ex);
    }
  });

  for (let source of sources) {
    console.log("handling source", source.location);
    source.dataInput.data(async (data) => {
      source.hasData = true;
      await writeFile(source.newLocation, data);

      if (sources.every((x) => x.hasData)) {
        await map();
      } else {
        console.error("Cannot start mapping, not all data has been received");
      }
    });
  }

  return () => console.log("RML started");
}
