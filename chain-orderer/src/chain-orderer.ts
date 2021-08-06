import { Config } from "arangojs/connection";

import { Block, MasterChainBlock, ShardChainBlock } from "./bmt-db";
import { ChainRange } from "./chain-range";
import { ChainRangesDb } from "./chain-ranges-db";
import { ChainRangesVerificationDb } from "./chain-ranges-verification-db";
import { DistributedBmtDb } from "./distributed-bmt-db";
import { Reporter } from "./reporter";
import { toU64String } from "./u64string";

export class ChainOrderer {
    readonly distributed_bmt_db: DistributedBmtDb;
    readonly chain_ranges_verification_db: ChainRangesVerificationDb;
    readonly chain_ranges_db: ChainRangesDb;

    private constructor(distributed_bmt_db: DistributedBmtDb, 
                        chain_ranges_verification_db: ChainRangesVerificationDb,
                        chain_ranges_db: ChainRangesDb) {
        this.distributed_bmt_db = distributed_bmt_db;
        this.chain_ranges_verification_db = chain_ranges_verification_db;
        this.chain_ranges_db = chain_ranges_db;
    }

    static async create(config: ChainOrdererConfig): Promise<ChainOrderer> {
        if (!config.bmt_databases || !config.chain_ranges_database || !config.chain_ranges_verification_database) {
            throw new Error("There are parameters missing in config");
        }

        const distributed_bmt_db = await DistributedBmtDb.create(config.bmt_databases);
        const chain_ranges_verification_db = new ChainRangesVerificationDb(config.chain_ranges_verification_database);
        const chain_ranges_db = new ChainRangesDb(config.chain_ranges_database);

        return new ChainOrderer(distributed_bmt_db, chain_ranges_verification_db, chain_ranges_db);
    }

    async run(): Promise<void> {
        await this.init_databases_and_process_first_mc_block_if_needed();

        let max_mc_seq_no = this.distributed_bmt_db.get_max_mc_seq_no();
        const summary = await this.chain_ranges_verification_db.get_summary();
        let last_processed_mc_seq_no = summary.last_verified_master_seq_no;

        const reporter = new Reporter();
        let previous_mc_block: MasterChainBlock | null = null;
        while (last_processed_mc_seq_no < max_mc_seq_no) {
            const chain_range: ChainRange = 
                await this.get_chain_range_with_mc_seq_no(last_processed_mc_seq_no + 1, previous_mc_block);
            
            await this.process_chain_range(chain_range);

            last_processed_mc_seq_no++;
            reporter.report_step(last_processed_mc_seq_no);
            previous_mc_block = chain_range.master_block;

            if (Date.now() - this.distributed_bmt_db.last_refresh > 60000) {
                await this.distributed_bmt_db.refresh_databases();
                max_mc_seq_no = this.distributed_bmt_db.get_max_mc_seq_no();
            }
        }

        reporter.report_finish(last_processed_mc_seq_no);
    }

    private async init_databases_and_process_first_mc_block_if_needed(): Promise<void> {
        await this.chain_ranges_verification_db.init_summary_if_not_exists({
            reliabe_chain_order_upper_boundary: "01",
            last_verified_master_seq_no: 0,
        });

        await this.chain_ranges_db.ensure_collection_ready();

        const summary = await this.chain_ranges_verification_db.get_summary();
        if (summary.last_verified_master_seq_no == 0) {
            const mc_block = await this.distributed_bmt_db.get_masterchain_block_by_seq_no(1);
            const chain_range: ChainRange = {
                master_block: mc_block,
                shard_blocks: [],
            };
            await this.process_chain_range(chain_range);
        }
    }

    private async process_chain_range(chain_range: ChainRange): Promise<void> {
        await this.chain_ranges_db.add_range(chain_range);
        await this.set_chain_order_for_range(chain_range);
        await this.update_summary(chain_range);
    }

    private async get_chain_range_with_mc_seq_no(mc_seq_no: number, previous_mc_block: MasterChainBlock | null): Promise<ChainRange> {
        previous_mc_block ??= await this.distributed_bmt_db.get_masterchain_block_by_seq_no(mc_seq_no - 1);
        const current_mc_block = await this.distributed_bmt_db.get_masterchain_block_by_seq_no(mc_seq_no);
        
        const blocks = [] as ShardChainBlock[];
        
        const root_shardchain_block_ids = new Map<string, boolean>(previous_mc_block.shard_block_ids.map(id => [id, true]));
        const top_shardchain_block_ids = current_mc_block.shard_block_ids;
        let shardchain_block_ids_to_get = top_shardchain_block_ids.filter(id => !root_shardchain_block_ids.has(id));

        let current_depth = 0;
        const max_depth = 10;
        while (shardchain_block_ids_to_get.length > 0) {
            if (current_depth > max_depth) {
                throw new Error(`Max shard search depth (${max_depth}) exceeded`);
            }

            const current_blocks = await this.distributed_bmt_db
                .get_shardchain_blocks_by_ids(shardchain_block_ids_to_get, current_mc_block.gen_utime);
                
            blocks.push(...current_blocks);

            shardchain_block_ids_to_get = []
            current_blocks.forEach(b => {
                if (!root_shardchain_block_ids.has(b.prev_block_id)) {
                    shardchain_block_ids_to_get.push(b.prev_block_id);
                }

                if (b.prev_alt_block_id && !root_shardchain_block_ids.has(b.prev_alt_block_id)) {
                    shardchain_block_ids_to_get.push(b.prev_alt_block_id);
                }
            })

            current_depth++;
        }

        return {
            master_block: current_mc_block,
            shard_blocks: blocks,
        };
    }

    private async set_chain_order_for_range(chain_range: ChainRange): Promise<void> {
        if (chain_range.master_block.chain_order && 
            !chain_range.shard_blocks.find(b => !b.chain_order)) {
            // we reached the blocks chain-ordered by parser or
            // the script failed between setting chain order and
            // updating summary
            await this.verify_chain_range(chain_range);
            return;
        }

        const master_order = toU64String(chain_range.master_block.seq_no);

        for (let b_i = 0; b_i < chain_range.shard_blocks.length; b_i++) {
            const block = chain_range.shard_blocks[b_i];

            if (block.chain_order) {
                continue;
            }
            
            const workchain_order = toU64String(block.workchain_id);
            const seq_no_order = toU64String(block.seq_no);
            const shard_order = shard_to_reversed_to_U64String(block.shard);
            const inner_order = `${workchain_order}${seq_no_order}${shard_order}`;
            const block_order = `${master_order}${inner_order}`; 
            
            await this.set_chain_order_for_block_transactions(block, block_order);
            await this.distributed_bmt_db.set_chain_order_for_block(block, block_order);
        }

        const master_block_order = master_order + "m";
        await this.set_chain_order_for_block_transactions(chain_range.master_block, master_block_order);
        await this.distributed_bmt_db.set_chain_order_for_block(chain_range.master_block, master_block_order);
    }

    private async verify_chain_range(chain_range: ChainRange) {        
        for (let b_i = 0; b_i < chain_range.shard_blocks.length; b_i++) {
            const block = chain_range.shard_blocks[b_i];
            await this.distributed_bmt_db.verify_block_and_transactions(block);
        }

        await this.distributed_bmt_db.verify_block_and_transactions(chain_range.master_block);
    }

    private async set_chain_order_for_block_transactions(block: Block, block_order: string): Promise<void> {
        const transactions = [...block.transactions];
        transactions.sort((t1, t2) => {
            if (t1.lt < t2.lt) {
                return -1;
            }
            if (t1.lt > t2.lt) {
                return 1;
            }
            
            if (t1.account_addr < t2.account_addr) {
                return -1;
            }
            if (t1.account_addr > t2.account_addr) {
                return 1;
            }

            throw new Error(`Duplicate transaction lt (${t1.lt}) and account_addr (${t1.account_addr})`);
        });

        const transaction_chain_orders = transactions.map((t, index) => {
            return {
                id: t.id,
                chain_order: `${block_order}${toU64String(index)}`,
            };
        });

        await this.distributed_bmt_db.set_transaction_chain_orders(transaction_chain_orders, block.gen_utime);
    }

    private async update_summary(chain_range: ChainRange): Promise<void> {
        await this.chain_ranges_verification_db.update_verifyed_boundary(chain_range.master_block.seq_no);
    }
}

export type ChainOrdererConfig = {
    bmt_databases: Config[],
    chain_ranges_database: Config,
    chain_ranges_verification_database: Config,
}

function shard_to_reversed_to_U64String(shard: string) {
    let result = "";
    for (let i = 0; i < shard.length; i++) {
        const hex_symbol = shard[shard.length - i - 1];
        
        if (result.length == 0 && hex_symbol == "0") {
            continue; // skip zeros from the end
        }

        const reverse_hex_symbol = reverse_hex_symbol_map.get(hex_symbol);

        if (!reverse_hex_symbol) {
            throw new Error(`Reverse hex for ${hex_symbol} not found while processing shard ${shard}`);
        }

        result = `${result}${reverse_hex_symbol}`;
    }

    result = (result.length - 1).toString(16) + result;
    return result;
}

const reverse_hex_symbol_map = (function () {
    const result = new Map<string, string>();
    for (let i = 0; i < 16; i++) {
        const bit_string = i.toString(2).padStart(4, "0");
        const splitted = bit_string.split("");
        const reversedSplitted = splitted.reverse();
        const reversed_string = reversedSplitted.join("");
        const reversed_number = parseInt(reversed_string, 2);
        
        result.set(i.toString(16), reversed_number.toString(16));
    }

    return result;
})();
