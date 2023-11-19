import type { Stream, Writer } from "@ajuvercr/js-runner";
import { exec } from "child_process";
import { readFile, writeFile } from "fs/promises";
import {
    DataFactory,
    Parser,
    Quad_Object,
    Quad_Predicate,
    Quad_Subject,
    Store,
    Writer as N3Writer,
} from "n3";
import { randomUUID, getJarFile } from "../util";
import { Cont, empty, match, pred, subject } from "rdf-lens";
import { RDF } from "@treecg/types";
import { RML, RMLT, VOID } from "../voc";

const { literal } = DataFactory;

const RML_MAPPER_RELEASE =
    "https://github.com/RMLio/rmlmapper-java/releases/download/v6.2.2/rmlmapper-6.2.2-r371-all.jar";

export type Source = {
    location: string;
    dataInput: Stream<string>;
    newLocation: string;
    hasData: boolean;
    trigger: boolean;
};

export type Target = {
    location: string;
    writer: Writer<string>;
    newLocation: string;
};

type SourceDataUpdate = {
    source: Source,
    data: string
};

export async function rmlMapper(
    mappingReader: Stream<string>,
    sources?: Source[],
    targets?: Target[],
    defaultWriter?: Writer<string>,
    jarLocation?: string,
) {
    const uid = randomUUID();
    const outputFile = "/tmp/rml-" + uid + "-output.ttl";
    const sourceBuffer: SourceDataUpdate[] = [];
    let executing: boolean = false;

    // Iterate over declared logical sources and organize them in a temporal location
    if (sources) {
        for (let source of sources) {
            const filename = source.location.split("/").pop();
            source.newLocation = `/tmp/rml-${uid}-input-${randomUUID()}-${filename}`;
            source.hasData = false;
            source.trigger = !!source.trigger;
        }
    }

    // Iterate over declared logical targets and organize them in a temporal location
    if (targets) {
        for (let target of targets) {
            const filename = target.location.split("/").pop();
            target.newLocation = `/tmp/rml-${uid}-output-${randomUUID()}-${filename}`;
        }
    }

    const jarFile = await getJarFile(jarLocation, false, RML_MAPPER_RELEASE);
    const mappingLocations: string[] = [];

    // Read mapping input stream
    mappingReader.data(async (input) => {
        console.log("[rmlMapper processor]", "Got mapping input!");
        try {
            const newMapping = transformMapping(input, sources, targets);
            const newLocation = `/tmp/rml-${uid}-mapping-${mappingLocations.length}.ttl`;
            await writeFile(newLocation, newMapping, { encoding: "utf8" });
            mappingLocations.push(newLocation);
            console.log("[rmlMapper processor]", "Added new mapping file location", newLocation);

        } catch (ex) {
            console.error("[rmlMapper processor]", "Could not map incoming rml input");
            console.error(ex);
        }
    }).on("end", async () => {
        // We assume mappings to be static and only proceed to execute them once we have them all
        if (sources) {
            for (let source of sources) {
                console.log("[rmlMapper processor]", "Handling source", source.location);
                // Process raw data input streams
                const handleSourceData = async (update: SourceDataUpdate) => {
                    const { source, data } = update;
                    console.log("[rmlMapper processor]", "Got data for", source.location);

                    if (executing) {
                        // We are already running a mapping process. 
                        // Store this date update and process when it is finished.
                        console.log("[rmlMapper processor]", "Buffering input until previous mapping is finished");
                        sourceBuffer.push({ source, data });
                    } else {
                        source.hasData = true;
                        await writeFile(source.newLocation, data);

                        if (sources.every((x) => x.hasData)) {
                            // We made sure that all declared logical sources are present
                            console.log("[rmlMapper processor]", "Start mapping now");
                            // Flag that mapping process is ongoing
                            executing = true;
                            await executeMappings(
                                mappingLocations,
                                jarFile,
                                outputFile,
                                sources,
                                targets,
                                defaultWriter
                            );
                            // Flag that mapping process is over
                            executing = false;
                            // Process buffered input updates
                            while (sourceBuffer.length > 0) {
                                const update = sourceBuffer.shift();
                                if (update) {
                                    console.log("[rmlMapper processor]", "Processing buffered input", update.source.location);
                                    await handleSourceData(update);
                                }
                            }
                        } else {
                            console.warn("[rmlMapper processor]", "Cannot start mapping, not all data has been received");
                        }
                    }
                };

                // Register data event handler
                source.dataInput.data(async (data) => await handleSourceData({ source, data }));
                // Process data that has already been pushed to the input stream
                if (source.dataInput.lastElement) {
                    await handleSourceData({ source, data: source.dataInput.lastElement });
                }
            }
        } else {
            // No declared logical sources means that raw data access is delegated to the RML engine.
            // For example, as in the case of remote RDBs or HTTP APIs
            console.log("[rmlMapper processor]", "Start mapping now");
            await executeMappings(
                mappingLocations,
                jarFile,
                outputFile,
                sources,
                targets,
                defaultWriter
            );
        }
    });
}

function transformMapping(input: string, sources?: Source[], targets?: Target[],) {
    const quads = new Parser().parse(input);
    const store = new Store(quads);

    const targetLens2 = empty<Cont>()
        .map(({ id }) => ({ subject: id }))
        .and(
            pred(VOID.terms.dataDump)
                .one()
                .map(({ id }) => ({ target: id })),
        )
        .map(([{ subject }, { target }]) => ({ subject, target }));

    const extractTarget = pred(RMLT.terms.target).one().then(targetLens2);

    const targetLens = match(undefined, RDF.terms.type, RMLT.terms.LogicalTarget)
        .thenAll(subject)
        .thenAll(extractTarget);
    const foundTargets = targetLens.execute(quads);

    for (let foundTarget of foundTargets) {
        if (targets) {
            let found = false;
            for (let target of targets) {
                if (target.location === foundTarget.target.value) {
                    console.log(
                        "[rmlMapper processor]",
                        "Moving location",
                        foundTarget.target.value,
                        "to",
                        target.newLocation,
                    );
                    found = true;
                    // Remove the old location
                    store.removeQuad(
                        <Quad_Subject>foundTarget.subject,
                        <Quad_Predicate>VOID.terms.dataDump,
                        <Quad_Object>foundTarget.target,
                    );

                    // Add the new location
                    store.addQuad(
                        <Quad_Subject>foundTarget.subject,
                        <Quad_Predicate>VOID.terms.dataDump,
                        literal("file://" + target.newLocation),
                    );
                    break;
                }
            }
            if (!found) {
                console.warn(
                    "[rmlMapper processor]",
                    `Logical Target ${foundTarget.subject.value} has no Connector Architecture declaration`
                );
            }
        }
    }

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

    // There exists a source that has no defined source, we cannot map this mapping
    for (let foundSource of foundSources) {
        if (sources) {
            let found = false;
            for (let source of sources) {
                if (source.location === foundSource.source) {
                    console.log(
                        "[rmlMapper processor]",
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
                console.warn(
                    "[rmlMapper processor]",
                    `Logical Source ${foundSource.subject.value} has no Connector Architecture declaration`
                );
            }
        }
    }

    return new N3Writer().quadsToString(store.getQuads(null, null, null, null));
}

async function executeMappings(
    mappingLocations: string[],
    jarFile: string,
    outputFile: string,
    sources?: Source[],
    targets?: Target[],
    defaultWriter?: Writer<string>
) {
    if (sources) {
        for (let source of sources) {
            // Reset the hasData property so it requires new data before it can map again
            // Useful when multiple sources need to update
            if (source.trigger) {
                source.hasData = false;
            }
        }
    }

    let out = "";
    for (let mappingFile of mappingLocations) {
        const t0 = new Date();
        console.log("[rmlMapper processor]", "Running", mappingFile);
        const command = `java -jar ${jarFile} -m ${mappingFile} -o ${outputFile}`;

        const proc = exec(command);
        proc.stdout!.on("data", function (data) {
            console.log("[rmlMapper processor]", "rml mapper std: ", data.toString());
        });
        proc.stderr!.on("data", function (data) {
            console.error("[rmlMapper processor]", "rml mapper err:", data.toString());
        });
        await new Promise((res) => proc.on("exit", res));

        out += await readFile(outputFile, { encoding: "utf8" });

        // TODO: 
        // Not all produced triples should go to all declared logical targets.
        // Logical targets are defined per SubjectMap, 
        // therefore we need to pair them up accordingly.
        if (targets) {
            for (let target of targets) {
                const file = await readFile(target.newLocation, { encoding: "utf8" });
                await target.writer.push(file);
            }
        }
        console.log("[rmlMapper processor]", "Done", mappingFile, `in ${new Date().getTime() - t0.getTime()} ms`);
    }

    console.log("[rmlMapper processor]", "All done");
    if (defaultWriter) {
        await defaultWriter.push(out);
    }
}


