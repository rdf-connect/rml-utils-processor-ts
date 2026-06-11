import { createLogger, transports } from "winston";
import { DataFactory } from "rdf-data-factory";
import { Reader } from "@rdfc/js-runner";

export const TEST_LOGGER = createLogger({
    transports: new transports.Console({
        level: "debug",
    }),
});

export const DF = new DataFactory();

export async function readChannel(reader: Reader) {
    const out: string[] = [];

    for await (const st of reader.strings()) {
        out.push(st);
    }

    return out;
}