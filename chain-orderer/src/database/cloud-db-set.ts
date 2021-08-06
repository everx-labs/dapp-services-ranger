import { Config } from "arangojs/connection";
import { ChainRangesDb } from "./chain-ranges-db";
import { ChainRangesVerificationDb } from "./chain-ranges-verification-db";
import { DistributedBmtDb } from "./distributed-bmt-db";

export class CloudDbSet {
    readonly distributed_bmt_db: DistributedBmtDb;
    readonly chain_ranges_verification_db: ChainRangesVerificationDb;
    readonly chain_ranges_db: ChainRangesDb;

    private constructor(
        distributed_bmt_db: DistributedBmtDb, 
        chain_ranges_verification_db: ChainRangesVerificationDb,
        chain_ranges_db: ChainRangesDb
    ) {
        this.distributed_bmt_db = distributed_bmt_db;
        this.chain_ranges_verification_db = chain_ranges_verification_db;
        this.chain_ranges_db = chain_ranges_db;
    }

    static async create(config: CloudDbSetConfig): Promise<CloudDbSet> {
        if (!config.bmt_databases || !config.chain_ranges_database || !config.chain_ranges_verification_database) {
            throw new Error("There are parameters missing in config");
        }

        const distributed_bmt_db = await DistributedBmtDb.create(config.bmt_databases);
        const chain_ranges_verification_db = new ChainRangesVerificationDb(config.chain_ranges_verification_database);
        const chain_ranges_db = new ChainRangesDb(config.chain_ranges_database);

        return new CloudDbSet(distributed_bmt_db, chain_ranges_verification_db, chain_ranges_db);
    }

    async get_max_mc_seq_no(params: { max_cache_age_ms: number }): Promise<number> {
        if (Date.now() - this.distributed_bmt_db.last_refresh > params.max_cache_age_ms) {
            await this.distributed_bmt_db.refresh_databases();
        }

        return this.distributed_bmt_db.get_max_mc_seq_no();
    }

    async ensure_chain_range_dbs_ready(): Promise<void> {
        await this.chain_ranges_verification_db.init_summary_if_not_exists({
            reliabe_chain_order_upper_boundary: "01",
            last_verified_master_seq_no: 0,
        });

        await this.chain_ranges_db.ensure_collection_ready();
    }
}

export type CloudDbSetConfig = {
    bmt_databases: Config[],
    chain_ranges_database: Config,
    chain_ranges_verification_database: Config,
}
