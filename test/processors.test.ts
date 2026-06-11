import { describe, expect, test } from "vitest";
import { FullProc as ProcInstance, Processor } from "@rdfc/js-runner";
import { ProcHelper } from "@rdfc/js-runner/lib/testUtils";
import { resolve } from "path";
import { pathToFileURL } from "url";
import { Yarrrml2RML } from "../src/yarrrml/yarrrml";
import { RMLMapperJS } from "../src/rml/rml";
import { RML2IncRML } from "../src/rml/incrml";

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
            "@prefix rdfc: <https://w3id.org/rdf-connect#>." + config,
        );
        const definedConfig = helper.getConfig(ty);

        expect(definedConfig.location).toBeDefined();
        expect(definedConfig.file).toBeDefined();
        expect(definedConfig.clazz).toBeDefined();

        const processor = await helper.getProcessor(uri);

        return processor;
    }

    test("rdfc:Yarrrml2RML is properly defined", async () => {
        const proc = await getProc<Yarrrml2RML>(
            `<http://example.com/ns#processor> a rdfc:Yarrrml2RML;
                rdfc:input <reader>;
                rdfc:output <writer>.`,
            "Yarrrml2RML",
        );

        expect(proc.constructor.name).toBe(Yarrrml2RML.name);
        expect(proc.reader.constructor.name).toBe("ReaderInstance");
        expect(proc.writer.constructor.name).toBe("WriterInstance");
    });

    test("rdfc:RMLMapperJS is properly defined", async () => {
        const proc = await getProc<RMLMapperJS>(
            `<http://example.com/ns#processor> a rdfc:RMLMapperJS;
                rdfc:mappings <reader>;
                rdfc:output <writer>.`,
            "RMLMapperJS",
        );

        expect(proc.constructor.name).toBe(RMLMapperJS.name);
        expect(proc.mappingInput.constructor.name).toBe("ReaderInstance");
        expect(proc.defaultWriter.constructor.name).toBe("WriterInstance");
    });

    test("rdfc:IncRMLTransformer is properly defined", async () => {
        const proc = await getProc<RML2IncRML>(
            `<http://example.com/ns#processor> a rdfc:IncRMLTransformer;
                rdfc:rmlStream <reader>;
                rdfc:config [
                    rdfc:stateBasePath <./state>;
                    rdfc:lifeCycleConfig [
                        rdfc:predicate <http://ex.org/lifeCycleProp>;
                        rdfc:create [
                            rdfc:function <https://w3id.org/imec/idlab/function#explicitCreate>;
                            rdfc:type <https://www.w3.org/ns/activitystreams#Create>
                        ];
                        rdfc:update [
                            rdfc:function <https://w3id.org/imec/idlab/function#implicitUpdate>;
                            rdfc:type <https://www.w3.org/ns/activitystreams#Update>
                        ];
                        rdfc:delete [
                            rdfc:function <https://w3id.org/imec/idlab/function#implicitDelete>;
                            rdfc:type <https://www.w3.org/ns/activitystreams#Delete>
                        ]
                    ]
                ];
                rdfc:incrmlStream <writer>.`,
            "IncRMLTransformer",
        );

        expect(proc.constructor.name).toBe(RML2IncRML.name);
        expect(proc.rmlStream.constructor.name).toBe("ReaderInstance");
        expect(proc.incrmlStream.constructor.name).toBe("WriterInstance");
        expect(proc.config.stateBasePath).toBe(pathToFileURL(resolve("./state")).href);
        expect(proc.config.lifeCycleConfig.predicate).toBe("http://ex.org/lifeCycleProp");
    });
});
