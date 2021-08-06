import { Config } from "arangojs/connection";
import { Database, aql } from "arangojs";
import { ChainRange } from "./chain-range";

export class ChainRangesDb {
    readonly config: Config;
    readonly database: Database;

    constructor(config: Config) {
        this.config = config;
        this.database = new Database(config);
    }
    
    async add_range(chain_range: ChainRange): Promise<void> {
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
