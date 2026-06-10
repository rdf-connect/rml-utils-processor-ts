import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import Y2R from '@rmlio/yarrrml-parser/lib/rml-generator.js';
import { Writer as RDFWriter } from 'n3';

type Yarrrml2RMLArgs = {
    reader: Reader;
    writer: Writer;
};

export class Yarrrml2RML extends Processor<Yarrrml2RMLArgs> {
    
    async init(this: Yarrrml2RMLArgs & this): Promise<void> {
        // nothing
    }

    async transform(this: Yarrrml2RMLArgs & this): Promise<void> {
        let counter = 0;
        const rdfParser = new RDFWriter();

        for await (const mapping of this.reader.strings()) {
            const y2r = new Y2R();
            const triples = y2r.convert(mapping);

            const rdfString = rdfParser.quadsToString(triples);

            this.logger.debug(`Produced RML triples:\n${rdfString}`);
            await this.writer.string(rdfString);
            counter++;
        }

        this.logger.info(`Transformed ${counter} YARRRML mappings.`);
        await this.writer.close();
    }

    async produce(this: Yarrrml2RMLArgs & this): Promise<void> {
        // nothing
    }
}