import { Config } from "arangojs/connection";

import { Bmt } from "./bmt";
import { ChainRangesDb } from "./chain-ranges-db";
import { ChainRangesVerificationDb } from "./chain-ranges-verification-db";
import { Block, DistributedBmtDb, MasterChainBlock, ShardChainBlock } from "./distributed-bmt-db";
import { toU64String } from "./u64string";

export class ChainOrderer {
    dBmtDb: DistributedBmtDb;
    chainRangesVerificationDb: ChainRangesVerificationDb;
    chainRangesDb: ChainRangesDb;

    constructor(config: ChainOrdererConfig) {
        if (!config.bmt_databases || !config.chain_ranges_database || !config.chain_ranges_verifying_database) {
            throw new Error("There are parameters missing in config");
        }

        this.dBmtDb = new DistributedBmtDb(config.bmt_databases);
        this.chainRangesVerificationDb = new ChainRangesVerificationDb(config.chain_ranges_verifying_database);
        this.chainRangesDb = new ChainRangesDb(config.chain_ranges_database);
    }

    async run() {
        await this.init_databases_and_process_first_mc_block_if_needed();

        const max_mc_seq_no = await this.dBmtDb.get_max_mc_seq_no();
        const summary = await this.chainRangesVerificationDb.get_summary();
        let last_processed_mc_seq_no = summary.last_verified_master_seq_no;

        let last_report = Date.now();
        let previous_mc_block: MasterChainBlock | null = null;
        while (last_processed_mc_seq_no < max_mc_seq_no) {
            const bmt: Bmt = await this.get_BMT_with_mc_seq_no(last_processed_mc_seq_no + 1, previous_mc_block);
            await this.chainRangesDb.add_range(bmt);
            await this.set_chain_order_for_bmt(bmt);
            await this.update_summary(bmt);

            last_processed_mc_seq_no++;
            previous_mc_block = bmt.master_block;

            if (Date.now() - last_report >= 1000) {
                last_report = Date.now();
                console.log(last_processed_mc_seq_no);
            }
        }

        console.log(`Finished at seq_no ${last_processed_mc_seq_no}`);
    }

    private async init_databases_and_process_first_mc_block_if_needed(): Promise<void> {
        await this.chainRangesVerificationDb.init_summary_if_not_existant({
            reliabe_chain_order_upper_boundary: "01",
            last_verified_master_seq_no: 0,
            workchain_ids: [-1, 0],
        });

        await this.chainRangesDb.ensure_collection_exists();

        const summary = await this.chainRangesVerificationDb.get_summary();
        if (summary.last_verified_master_seq_no == 0) {
            const mc_block = await this.dBmtDb.get_masterchain_block_by_seq_no(1);
            const bmt: Bmt = {
                master_block: mc_block,
                shard_blocks: [],
            };
            await this.chainRangesDb.add_range(bmt);
            await this.set_chain_order_for_bmt(bmt);
            await this.update_summary(bmt);
        }
    }

    private async get_BMT_with_mc_seq_no(mc_seq_no: number, previous_mc_block: MasterChainBlock | null): Promise<Bmt> {
        previous_mc_block ??= await this.dBmtDb.get_masterchain_block_by_seq_no(mc_seq_no - 1);
        const current_mc_block = await this.dBmtDb.get_masterchain_block_by_seq_no(mc_seq_no);
        
        let blocks = [] as ShardChainBlock[];
        
        const root_shardchain_block_ids = new Map<string, boolean>(previous_mc_block.shard_block_ids.map(id => [id, true]));
        const top_shardchain_block_ids = current_mc_block.shard_block_ids;
        let shardchain_block_ids_to_get = top_shardchain_block_ids.filter(id => !root_shardchain_block_ids.has(id));

        let current_depth = 0;
        const max_depth = 10;
        while (shardchain_block_ids_to_get.length > 0) {
            if (current_depth > max_depth) {
                throw new Error(`Max shard search depth (${max_depth}) exceeded`);
            }

            let current_blocks = await this.dBmtDb.get_shardchain_blocks_by_ids(shardchain_block_ids_to_get, current_mc_block.gen_utime);
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

    private async set_chain_order_for_bmt(bmt: Bmt): Promise<void> {
        if (bmt.master_block.chain_order && 
            bmt.shard_blocks.reduce((ok, block) => ok && !!block.chain_order, true)) {
            // we reached the blocks chain-ordered by parser or
            // the script failed between setting chain order and
            // updating summary
            this.verify_bmt(bmt);
            return;
        }

        const master_order = toU64String(bmt.master_block.seq_no);

        for (let b_i = 0; b_i < bmt.shard_blocks.length; b_i++) {
            const block = bmt.shard_blocks[b_i];

            if (block.chain_order) {
                continue;
            }
            
            const workchain_order = toU64String(block.workchain_id);
            const seq_no_order = toU64String(block.seq_no);
            const shard_order = shard_to_reversed_to_U64String(block.shard);
            const inner_order = workchain_order + seq_no_order + shard_order;
            const block_order = master_order + inner_order; 
            
            await this.set_chain_order_for_block_transactions(block, block_order);
            await this.dBmtDb.set_chain_order_for_block(block, block_order);
        }

        const master_block_order = master_order + "m";
        await this.set_chain_order_for_block_transactions(bmt.master_block, master_block_order);
        await this.dBmtDb.set_chain_order_for_block(bmt.master_block, master_block_order);
    }

    private async verify_bmt(bmt: Bmt) {        
        for (let b_i = 0; b_i < bmt.shard_blocks.length; b_i++) {
            const block = bmt.shard_blocks[b_i];
            this.dBmtDb.verify_block_and_transactions(block);
        }

        this.dBmtDb.verify_block_and_transactions(bmt.master_block);
    }

    private async set_chain_order_for_block_transactions(block: Block, block_order: string): Promise<void> {
        const transactions = block.transactions;
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
                chain_order: block_order + toU64String(index),
            };
        });

        await this.dBmtDb.set_transaction_chain_orders(transaction_chain_orders, block.gen_utime);
    }

    private async update_summary(bmt: Bmt): Promise<void> {
        await this.chainRangesVerificationDb.update_verifyed_boundary(bmt.master_block.seq_no);
    }
}

export type ChainOrdererConfig = {
    bmt_databases: Config[],
    chain_ranges_database: Config,
    chain_ranges_verifying_database: Config,
}

function shard_to_reversed_to_U64String(shard: string) {
    let result = "";
    for (let i = 0; i < shard.length; i++) {
        const hex_symbol = shard[shard.length - i - 1];
        
        if (result.length == 0 && hex_symbol == "0") 
            continue; // skip zeros from the end

        result = result + reverse_hex_symbol_map.get(hex_symbol);
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
        const reversed_number = +("0b" + reversed_string);
        
        result.set(i.toString(16), reversed_number.toString(16));
    }

    return result;
})();
