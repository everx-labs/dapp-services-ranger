import { Config } from "arangojs/connection";
import { Database, aql } from "arangojs";
import { Bmt } from "./bmt";

export class ChainRangesDb {
    readonly config: Config;
    readonly database: Database;

    constructor(config: Config) {
        this.config = config;
        this.database = new Database(config);
    }
    
    async add_range(bmt: Bmt): Promise<void> {
        const range = {
            _key: bmt.master_block.id,
            master_block: {
                id: bmt.master_block.id,
                seq_no: bmt.master_block.seq_no,
            },
            shard_block_ids: bmt.shard_blocks.map(sb => sb.id),
        }
        const query = aql`
            INSERT ${range} 
            INTO chain_ranges
            OPTIONS { waitForSync: true, overwriteMode: "ignore" }
        `;

        await this.database.query(query);
    }

    async ensure_collection_exists(): Promise<void> {
        if (!await this.database.collection("chain_ranges").exists()) {
            await this.database.createCollection("chain_ranges");
            await this.database.collection("chain_ranges").ensureIndex({
                type: "persistent",
                fields: [ "master_block.seq_no" ],
            });
        }
    }
}
