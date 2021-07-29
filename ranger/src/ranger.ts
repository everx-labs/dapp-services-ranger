import { Config } from "arangojs/connection";
import { Block, BmtDb } from "./bmt-db";
import { ChainRange, ChainRangesDb } from "./chain-ranges-db";
import { ChainRangesVerificationDb } from "./chain-ranges-verification-db";

export class Ranger {
    readonly bmt_db: BmtDb;
    readonly chain_ranges_verification_db: ChainRangesVerificationDb;
    readonly chain_ranges_db: ChainRangesDb;
    readonly database_polling_period: number;
    readonly max_transaction_ids_per_request: number;

    constructor(config: RangerConfig) {
        this.bmt_db = new BmtDb(config.bmt_database);
        this.chain_ranges_verification_db = new ChainRangesVerificationDb(config.chain_ranges_verification_database);
        this.chain_ranges_db = new ChainRangesDb(config.chain_ranges_database);
        this.database_polling_period = config.database_polling_period ?? 1000;
        this.max_transaction_ids_per_request = config.max_transaction_ids_per_request ?? 10000;
    }

    async run(): Promise<void> {
        const summary = await this.chain_ranges_verification_db.get_summary();
        let last_verified_master_seq_no = summary.last_verified_master_seq_no;

        while (true) {
            const chain_range = await this.chain_ranges_db.get_range_by_mc_seq_no(last_verified_master_seq_no + 1);
            if (!chain_range) {
                await delay(this.database_polling_period);
                continue;
            }
            await this.ensure_chain_range_is_in_bmt_db(chain_range);
            await this.chain_ranges_verification_db.update_verifyed_boundary(last_verified_master_seq_no + 1);
            last_verified_master_seq_no++;
        }
    }

    private async ensure_chain_range_is_in_bmt_db(chain_range: ChainRange): Promise<void> {
        let block_ids_to_check = [chain_range.master_block.id, ...chain_range.shard_block_ids];
        let transcation_id_to_check = [] as string[];

        while (true) {
            const delayPromise = delay(this.database_polling_period);
            if (block_ids_to_check.length > 0) {
                const blocks = await this.bmt_db.find_blocks_by_ids(block_ids_to_check);
                this.ensure_chain_range_on_blocks(blocks);
                transcation_id_to_check = transcation_id_to_check.concat(...blocks.map(b => b.transaction_ids));
                block_ids_to_check = block_ids_to_check.filter(id => !blocks.find(b => b.id == id));
            }
            if (transcation_id_to_check.length > 0) {
                const transaction_ids = await this.find_existant_transaction_ids(transcation_id_to_check);
                transcation_id_to_check = transcation_id_to_check.filter(id => !transaction_ids.find(id2 => id2 == id));
            }

            if (block_ids_to_check.length == 0 && transcation_id_to_check.length == 0) {
                break;
            } else {
                await delayPromise;
            }
        }
    }

    private ensure_chain_range_on_blocks(blocks: Block[]): void {
        const blocks_without_co = blocks.filter(b => !b.chain_order);
        if (blocks_without_co.length > 0) {
            throw new Error(`There is no chain range on blocks: ${blocks_without_co.map(b => b.id).join(", ")}`);
        }
    }

    private async find_existant_transaction_ids(ids: string[]): Promise<string[]> {
        let start_index = 0;
        let result = [] as string[];
        while (start_index < ids.length) {
            const ids_to_check = ids.slice(start_index, start_index + this.max_transaction_ids_per_request);
            result = result.concat(await this.bmt_db.find_existant_transaction_ids(ids_to_check));
            start_index += this.max_transaction_ids_per_request;
        }

        return result;
    }
}

export type RangerConfig = {
    bmt_database: Config,
    chain_ranges_database: Config,
    chain_ranges_verification_database: Config,
    database_polling_period: number | undefined,
    max_transaction_ids_per_request: number | undefined
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setInterval(resolve, ms));
}
