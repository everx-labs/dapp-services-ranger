import { Config } from "arangojs/connection";

import { ChainOrderUtils } from "./chain-order-utils";
import { MasterChainBlock } from "./database/bmt-db";
import { ChainRangeExtended } from "./database/chain-range-extended";
import { CloudDbSet } from "./database/cloud-db-set";
import { Reporter } from "./reporter";

export class ChainOrderer {
    readonly db_set: CloudDbSet;

    private constructor(db_set: CloudDbSet) {
        this.db_set = db_set;
    }

    static async create(config: ChainOrdererConfig): Promise<ChainOrderer> {
        const db_set = await CloudDbSet.create(config);
        return new ChainOrderer(db_set);
    }

    async run(): Promise<void> {
        await this.init_databases_and_process_first_mc_block_if_needed();

        const summary = await this.db_set.chain_ranges_verification_db.get_summary();
        let last_processed_mc_seq_no = summary.last_verified_master_seq_no;

        const reporter = new Reporter();
        let previous_mc_block: MasterChainBlock | null = null;
        while (last_processed_mc_seq_no < await this.db_set.get_max_mc_seq_no({ max_cache_age_ms: 60000 })) {
            const chain_range: ChainRangeExtended = 
                await this.db_set.distributed_bmt_db
                    .get_chain_range_with_mc_seq_no(last_processed_mc_seq_no + 1, previous_mc_block);
            
            await this.process_chain_range(chain_range);

            last_processed_mc_seq_no++;
            reporter.report_step(last_processed_mc_seq_no);
            previous_mc_block = chain_range.master_block;
        }

        reporter.report_finish(last_processed_mc_seq_no);
    }

    private async init_databases_and_process_first_mc_block_if_needed(): Promise<void> {
        await this.db_set.ensure_chain_range_dbs_ready();

        const summary = await this.db_set.chain_ranges_verification_db.get_summary();
        if (summary.last_verified_master_seq_no == 0) {
            const mc_block = await this.db_set.distributed_bmt_db.get_masterchain_block_by_seq_no(1);
            const chain_range: ChainRangeExtended = {
                master_block: mc_block,
                shard_blocks: [],
            };
            await this.process_chain_range(chain_range);
        }
    }

    private async process_chain_range(chain_range: ChainRangeExtended): Promise<void> {
        await this.db_set.chain_ranges_db.add_range(chain_range);
        await this.set_chain_order_for_range(chain_range);
        await this.update_summary(chain_range);
    }

    private async set_chain_order_for_range(chain_range: ChainRangeExtended): Promise<void> {
        if (chain_range.master_block.chain_order && 
            !chain_range.shard_blocks.find(b => !b.chain_order)) {
            // we reached the blocks chain-ordered by parser or
            // the script failed between setting chain order and
            // updating summary
            await this.verify_chain_range(chain_range);
            return;
        }

        const chain_orders = ChainOrderUtils.get_chain_range_chain_orders(chain_range);

        for (let b_i = 0; b_i < chain_range.shard_blocks.length; b_i++) {
            const block = chain_range.shard_blocks[b_i];

            if (block.chain_order) {
                continue;
            }

            const block_chain_orders = chain_orders.shard_blocks.get(block.id);
            if (!block_chain_orders) {
                throw new Error("Impossible exception in set_chain_order_for_range");
            }

            await this.db_set.distributed_bmt_db.set_mt_chain_orders(
                block_chain_orders.messages,
                block_chain_orders.transactions, 
                block.gen_utime);
                
            await this.db_set.distributed_bmt_db.set_chain_order_for_block(block, block_chain_orders.chain_order);
        }

        await this.db_set.distributed_bmt_db.set_mt_chain_orders(
            chain_orders.master_block.messages,
            chain_orders.master_block.transactions, 
            chain_range.master_block.gen_utime);
            
        await this.db_set.distributed_bmt_db.set_chain_order_for_block(chain_range.master_block, chain_orders.master_block.chain_order);
    }

    private async verify_chain_range(chain_range: ChainRangeExtended) {        
        for (let b_i = 0; b_i < chain_range.shard_blocks.length; b_i++) {
            const block = chain_range.shard_blocks[b_i];
            await this.db_set.distributed_bmt_db.verify_bmt_existance_for_block(block);
        }

        await this.db_set.distributed_bmt_db.verify_bmt_existance_for_block(chain_range.master_block);
    }

    private async update_summary(chain_range: ChainRangeExtended): Promise<void> {
        await this.db_set.chain_ranges_verification_db.update_verifyed_boundary(chain_range.master_block.seq_no);
    }
}

export type ChainOrdererConfig = {
    bmt_databases: Config[],
    chain_ranges_database: Config,
    chain_ranges_verification_database: Config,
}
