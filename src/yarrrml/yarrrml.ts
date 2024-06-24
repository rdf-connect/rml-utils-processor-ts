import type { Stream, Writer } from "@rdfc/js-runner";
import Y2R from '@rmlio/yarrrml-parser/lib/rml-generator.js';
import { Writer as RDFWriter } from 'n3';

export function yarrrml2rml(reader: Stream<string>, writer: Writer<string>) {

    const handle = async (x: string) => {
        const y2r = new Y2R();
        const triples = y2r.convert(x);

        const str = new RDFWriter().quadsToString(triples);
        await writer.push(str);
    }

    reader.data(handle);
    reader.on("end", async () => {
        await writer.end();
    });
    if (reader.lastElement) {
        handle(reader.lastElement);
    }
}