import { Config } from "arangojs/connection";
import { Database, aql } from "arangojs";

export class ChainRangesDb {
    readonly config: Config;
    readonly database: Database;

    constructor(config: Config) {
        this.config = config;
        this.database = new Database(config);
    }
    
    async get_range_by_mc_seq_no(mc_seq_no: number): Promise<ChainRange | null> {
        const cursor = await this.database.query(aql`
            FOR cr IN chain_ranges
                FILTER cr.master_block.seq_no == ${mc_seq_no}
                RETURN cr
        `);

        const result = await cursor.next() as ChainRange | null;
        return result;        
    }
}

export type ChainRange = {
    _key: string,
    master_block: {
        id: string,
        seq_no: number,
    },
    shard_blocks_ids: string[],
}
