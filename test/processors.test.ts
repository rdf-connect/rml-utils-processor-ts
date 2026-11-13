import { describe, expect, test } from "vitest";
import { FullProc as ProcInstance, Processor } from "@rdfc/js-runner";
import { ProcHelper } from "@rdfc/js-runner/lib/testUtils";
import { resolve } from "path";
import { rmSync, writeFileSync } from "fs";
import { getJarFile } from "../src/util";
import { Yarrrml2RML } from "../src/yarrrml/yarrrml";
import { RMLMapperJS } from "../src/rml/rml";
import { RML2IncRML } from "../src/rml/incrml";
import { fromPath } from "@rdfc/utils-js";

describe("Tests for RML-related processors", async () => {
    async function getProc<T extends Processor<unknown>>(
        config: string,
        ty: string,
        uri = "http://example.com/ns#processor",
    ): Promise<ProcInstance<T>> {
        const helper = new ProcHelper<T>();

        await helper.importFile(resolve("./processors.ttl"));
        await helper.importInline(
            resolve("pipeline.ttl"),
            "@prefix rdfc: <https://w3id.org/rdf-connect#>. @prefix rdfcp: <https://w3id.org/rdf-connect/processors#>." + config,
        );
        const definedConfig = helper.getConfig(ty);

        expect(definedConfig.location).toBeDefined();
        expect(definedConfig.file).toBeDefined();
        expect(definedConfig.clazz).toBeDefined();

        const processor = await helper.getProcessor(uri);

        return processor;
    }

    test("rdfcp:Yarrrml2RML is properly defined", async () => {
        const proc = await getProc<Yarrrml2RML>(
            `<http://example.com/ns#processor> a rdfcp:Yarrrml2RML;
                rdfcp:input <reader>;
                rdfcp:output <writer>.`,
            "Yarrrml2RML",
        );

        expect(proc.constructor.name).toBe(Yarrrml2RML.name);
        expect(proc.reader.constructor.name).toBe("ReaderInstance");
        expect(proc.writer.constructor.name).toBe("WriterInstance");
    });

    test("rdfcp:RMLMapperJS is properly defined", async () => {
        const proc = await getProc<RMLMapperJS>(
            `<http://example.com/ns#processor> a rdfcp:RMLMapperJS;
                rdfcp:mappings <reader>;
                rdfcp:output <writer>.`,
            "RMLMapperJS",
        );

        expect(proc.constructor.name).toBe(RMLMapperJS.name);
        expect(proc.mappingInput.constructor.name).toBe("ReaderInstance");
        expect(proc.defaultWriter.constructor.name).toBe("WriterInstance");
    });

    test("rdfcp:IncRMLTransformer is properly defined", async () => {
        const proc = await getProc<RML2IncRML>(
            `<http://example.com/ns#processor> a rdfcp:IncRMLTransformer;
                rdfcp:rmlStream <reader>;
                rdfcp:config [
                    rdfcp:stateBasePath <./state>;
                    rdfcp:lifeCycleConfig [
                        rdfcp:predicate <http://ex.org/lifeCycleProp>;
                        rdfcp:create [
                            rdfcp:function <https://w3id.org/imec/idlab/function#explicitCreate>;
                            rdfcp:type <https://www.w3.org/ns/activitystreams#Create>
                        ];
                        rdfcp:update [
                            rdfcp:function <https://w3id.org/imec/idlab/function#implicitUpdate>;
                            rdfcp:type <https://www.w3.org/ns/activitystreams#Update>
                        ];
                        rdfcp:delete [
                            rdfcp:function <https://w3id.org/imec/idlab/function#implicitDelete>;
                            rdfcp:type <https://www.w3.org/ns/activitystreams#Delete>
                        ]
                    ]
                ];
                rdfcp:incrmlStream <writer>.`,
            "IncRMLTransformer",
        );

        expect(proc.constructor.name).toBe(RML2IncRML.name);
        expect(proc.rmlStream.constructor.name).toBe("ReaderInstance");
        expect(proc.incrmlStream.constructor.name).toBe("WriterInstance");
        expect(proc.config.stateBasePath).toBe(fromPath("./state"));
        expect(proc.config.lifeCycleConfig.predicate).toBe("http://ex.org/lifeCycleProp");
    });

    test("rdfcp:RMLMapperJS falls back to its default values", async () => {
        const proc = await getProc<RMLMapperJS>(
            `<http://example.com/ns#processor> a rdfcp:RMLMapperJS;
                rdfcp:mappings <reader>;
                rdfcp:output <writer>;
                rdfcp:rmlSource [
                    rdfcp:sourceLocation "dataset/data.xml";
                    rdfcp:input <sourceReader>
                ].`,
            "RMLMapperJS",
        );

        expect(proc.sources).toHaveLength(1);
        expect(proc.sources![0].location).toBe("dataset/data.xml");
        expect(proc.sources![0].trigger).toBe(false);
    });

    test("rdfcp:RMLMapperJS takes its jar and target locations written as IRIs", async () => {
        const proc = await getProc<RMLMapperJS>(
            `<http://example.com/ns#processor> a rdfcp:RMLMapperJS;
                rdfcp:mappings <reader>;
                rdfcp:output <writer>;
                rdfcp:rmlTarget [
                    rdfcp:targetLocation <file:///results/output.nq>;
                    rdfcp:output <targetWriter>
                ];
                rdfcp:rmlJar <./iri-test-rmlmapper.jar>.`,
            "RMLMapperJS",
        );

        // Matched as is against void:dataDump in the mapping, so not turned into a path
        expect(proc.targets![0].location).toBe("file:///results/output.nq");

        const jar = resolve("./iri-test-rmlmapper.jar");
        expect(proc.jarLocation).toBe(fromPath(jar));
        writeFileSync(jar, "");
        try {
            expect(await getJarFile(proc.jarLocation)).toBe(jar);
        } finally {
            rmSync(jar, { force: true });
        }
    });
});
