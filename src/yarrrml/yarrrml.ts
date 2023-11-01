import type { Stream, Writer } from "@ajuvercr/js-runner";
import Y2R from '@rmlio/yarrrml-parser/lib/rml-generator';
import { Writer as RDFWriter } from 'n3';

export function yarrrml2rml(reader: Stream<string>, writer: Writer<string>) {

    const handle = (x: string) => {
        const y2r = new Y2R();
        const triples = y2r.convert(x);

        const str = new RDFWriter().quadsToString(triples);
        return writer.push(str);
    }

    reader.data(handle);
    reader.on("end", async () => {
        await writer.end();
    });
    if (reader.lastElement) {
        handle(reader.lastElement);
    }
}