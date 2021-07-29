import { Config } from "arangojs/connection";
import { Database, aql } from "arangojs";
import { toU64String } from "./u64string";

export class ChainRangesVerificationDb {
    readonly config: Config;
    readonly database: Database;

    constructor(config: Config) {
        this.config = config;
        this.database = new Database(config);
    }
    
    async get_summary(): Promise<ChainRangesSummary> {
        const cursor = await this.database.query(aql`
            FOR doc IN chain_ranges_verification
                FILTER doc._key == "summary"
                RETURN doc
        `);
        const result = await cursor.next() as ChainRangesSummary | null;
        if (!result) {
            throw new Error("Chain ranges summary not found");
        }
        return result;
    }

    async update_verifyed_boundary(veryfied_master_seq_no: number): Promise<void> {
        const chain_order_boundary = toU64String(veryfied_master_seq_no + 1);

        await this.database.query(aql`
            UPDATE "summary" 
                WITH { 
                    reliabe_chain_order_upper_boundary: ${chain_order_boundary},
                    last_verified_master_seq_no: ${veryfied_master_seq_no},
                }
                IN chain_ranges_verification
                OPTIONS { waitForSync: true }
        `);
    }
}

export type ChainRangesSummary = {
    _key: "summary",
    reliabe_chain_order_upper_boundary: string, // for convenience of q-server
    last_verified_master_seq_no: number, // for convenience of ranger
    workchain_ids: number[], // configuration of ranger
}
