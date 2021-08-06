import { Config } from "arangojs/connection";
import { Database, aql } from "arangojs";
import { ChainRangeExtended } from "./chain-range-extended";

export class ChainRangesDb {
    readonly config: Config;
    readonly database: Database;

    constructor(config: Config) {
        this.config = config;
        this.database = new Database(config);
    }
    
    async add_range(chain_range: ChainRangeExtended): Promise<void> {
        const range = {
            _key: chain_range.master_block.id,
            master_block: {
                id: chain_range.master_block.id,
                seq_no: chain_range.master_block.seq_no,
            },
            shard_blocks_ids: chain_range.shard_blocks.map(sb => sb.id),
        }
        
        await this.database.query(aql`
            INSERT ${range} 
            INTO chain_ranges
            OPTIONS { waitForSync: true, overwriteMode: "ignore" }
        `);
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

    async ensure_collection_ready(): Promise<void> {
        if (!await this.database.collection("chain_ranges").exists()) {
            await this.database.createCollection("chain_ranges");
        }
        await this.database.collection("chain_ranges").ensureIndex({
            type: "persistent",
            fields: [ "master_block.seq_no" ],
        });
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
