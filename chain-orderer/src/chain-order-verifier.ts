import { Config } from "arangojs/connection";
import { ChainOrderUtils } from "./chain-order-utils";
import { Block, ChainOrderedTransaction, MasterChainBlock } from "./database/bmt-db";
import { ChainRangeExtended } from "./database/chain-range-extended";
import { CloudDbSet } from "./database/cloud-db-set";
import { Reporter } from "./reporter";

export class ChainOrderVerifier {
    readonly db_set: CloudDbSet;
    readonly start_seq_no: number;

    private constructor(db_set: CloudDbSet, start_seq_no: number) {
        this.db_set = db_set;
        this.start_seq_no = start_seq_no;
    }

    static async create(config: ChainOrderVerifierConfig): Promise<ChainOrderVerifier> {
        const db_set = await CloudDbSet.create(config);
        return new ChainOrderVerifier(db_set, config.only_verify_from_seq_no);
    }

    async run(): Promise<void> {
        let last_processed_mc_seq_no = this.start_seq_no - 1;

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

    private async process_chain_range(chain_range: ChainRangeExtended): Promise<void> {
        await this.verify_range_in_chain_ranges_db(chain_range);
        await this.verify_chain_order_for_range(chain_range);
    }

    async verify_range_in_chain_ranges_db(chain_range_ex: ChainRangeExtended): Promise<void> {
        const chain_range = await this.db_set.chain_ranges_db
            .get_range_by_mc_seq_no(chain_range_ex.master_block.seq_no);

        if (!chain_range) {
            throw new Error(`Chain range with seq_no ${chain_range_ex.master_block.seq_no} not found`);
        }
        if (chain_range.master_block.id != chain_range_ex.master_block.id) {
            throw new Error(`Chain range inconsistent for seq_no ${chain_range_ex.master_block.seq_no}: ` +
                `id ${chain_range.master_block.id} in chain_ranges and ${chain_range_ex.master_block.id} in blocks`);
        }
        const shard_block_ids_cr = [...chain_range.shard_blocks_ids];
        const shard_block_ids_b = chain_range_ex.shard_blocks.map(b => b.id);
        const shard_block_ids_cr_set = new Set(shard_block_ids_cr);
        const shard_block_ids_b_set = new Set(shard_block_ids_b);
        const cr_inconsistency = shard_block_ids_cr.filter(id => !shard_block_ids_b_set.has(id));
        const b_inconsistency = shard_block_ids_b.filter(id => !shard_block_ids_cr_set.has(id));
        if (cr_inconsistency.length || b_inconsistency.length) {
            throw new Error(`Chain range inconsistent for seq_no ${chain_range_ex.master_block.seq_no}:\n` +
                `${cr_inconsistency.length ? `chain_ranges has more shard_blocks: ${cr_inconsistency.join(', ')}\n` : ''}` +
                `${b_inconsistency.length ? `blocks has more shard_blocks: ${cr_inconsistency.join(', ')}\n` : ''}`)
        }
    }

    private async verify_chain_order_for_range(chain_range: ChainRangeExtended): Promise<void> {
        const chain_orders = ChainOrderUtils.get_chain_range_chain_orders(chain_range);

        for (const block of chain_range.shard_blocks) {
            const block_chain_orders = chain_orders.shard_blocks.get(block.id);
            if (!block_chain_orders) {
                throw new Error("Impossible exception in verify_chain_order_for_range");
            }

            if (block_chain_orders.chain_order != block.chain_order) {
                throw new Error(`Block ${block.id}: expected ${block_chain_orders.chain_order} but got ${block.chain_order ?? 'null'}`);
            }

            await this.verify_chain_orders_on_transactions(block, block_chain_orders.transactions);
        }

        if (chain_orders.master_block.chain_order != chain_range.master_block.chain_order) {
            throw new Error(`Block ${chain_range.master_block.id}: expected ${chain_orders.master_block.chain_order} but got ${chain_range.master_block.chain_order ?? 'null'}`);
        }

        await this.verify_chain_orders_on_transactions(chain_range.master_block, chain_orders.master_block.transactions);
    }
    
    async verify_chain_orders_on_transactions(block: Block, chain_ordered_transactions: ChainOrderedTransaction[]): Promise<void> {
        const transactions_chain_orders_fact = 
        await this.db_set.distributed_bmt_db
            .get_transactions_chain_orders_for_block(block);
    
        const transactions_chain_orders_expected =
            new Map(chain_ordered_transactions.map(t_co => [t_co.id, t_co.chain_order]));

        for (const t_co of transactions_chain_orders_fact) {
            const fact = t_co.chain_order;
            const expected = transactions_chain_orders_expected.get(t_co.id);
            if (fact != expected) {
                throw new Error(`Invalid chain_order for transaction ${t_co.id}: expected ${expected ?? 'null'} but got ${fact}`);
            }
        }
    }
}

export type ChainOrderVerifierConfig = {
    bmt_databases: Config[],
    chain_ranges_database: Config,
    chain_ranges_verification_database: Config,
    only_verify_from_seq_no: number,
}
