import { BMT_IDs } from "./bmt-types";
import { IBmtDb, MasterChainBlockInfo } from "./bmt-db";
import { RangerConfig } from "./ranger-config";

export class BlockWalker {
    static MAX_DEPTH = 10;
    readonly bmt_db: IBmtDb;
    readonly masterchain_block_id: string;

    masterchain_block_info?: MasterChainBlockInfo;
    prev_masterchain_block_info?: MasterChainBlockInfo;

    // Graph
    root_shardchain_block_ids?: ReadonlyArray<string>;
    top_shardchain_block_ids?: ReadonlyArray<string>;
    blocks?: Map<string, ShardChainBlockGraphNode>;
    
    result?: BMT_IDs;

    constructor(bmt_db: IBmtDb, masterchain_block_id: string) {
        this.bmt_db = bmt_db;
        this.masterchain_block_id = masterchain_block_id;
    }

    async run(): Promise<BMT_IDs> {
        await this.init_masterchain_block_info();
        await this.init_previous_masterchain_block_info();
        await this.init_graph();
        await this.walk_shardchain_block_and_fill_graph();
        await this.flatten_graph_and_prepare_result();
        return this.result!;
    }

    private async init_masterchain_block_info(): Promise<void> {
        this.masterchain_block_info = await this.bmt_db.get_masterchain_block_info_by_id(this.masterchain_block_id);

        if (!this.masterchain_block_info) {
            throw new Error(`Masterblock with id ${this.masterchain_block_id} not found in buffer DB`);
        }
    }

    private async init_previous_masterchain_block_info(): Promise<void> {
        const prev_masterblock_id = this.masterchain_block_info!.prev_block_id;
        
        this.prev_masterchain_block_info = await this.bmt_db.get_masterchain_block_info_by_id(prev_masterblock_id);
        
        if (!this.prev_masterchain_block_info) {
            throw new Error(`Masterblock with id ${prev_masterblock_id} not found in ordered DB`);
        }
    }

    private async init_graph(): Promise<void> {
        this.root_shardchain_block_ids = this.prev_masterchain_block_info!.shard_block_ids;
        this.top_shardchain_block_ids = this.masterchain_block_info!.shard_block_ids;

        this.blocks = new Map<string, ShardChainBlockGraphNode>();

        this.root_shardchain_block_ids!.forEach(sh_id => {
            this.blocks!.set(sh_id, {
                type: "Root",
                id: sh_id,
            });
        });
    }

    private async walk_shardchain_block_and_fill_graph(): Promise<void> {
        const blocks = this.blocks!;

        let next_shardchain_block_ids = this.top_shardchain_block_ids!
            .filter(id => !blocks.get(id)); // skip root blocks
        let current_depth = 0;

        // Init top nodes
        next_shardchain_block_ids.forEach(id => {
            blocks.set(id, {
                type: "Top",
                id
            });
        })

        while (next_shardchain_block_ids.length > 0) {
            if (current_depth > BlockWalker.MAX_DEPTH) {
                throw new Error(`Max search depth of ${BlockWalker.MAX_DEPTH} exceeded`);
            }

            let current_shardchain_block_ids = next_shardchain_block_ids;
            next_shardchain_block_ids = [];

            const current_blocks = await this.bmt_db.get_shardchain_block_infos_by_ids(current_shardchain_block_ids);
            if (current_blocks.length != current_shardchain_block_ids.length)
                throw new Error(`Some of the shardchain blocks not found while trying to get blocks with ids ${current_shardchain_block_ids}`);

            current_blocks.forEach(block_info => {
                const node = blocks.get(block_info._key) as TopShardChainBlockGraphNode | MidShardChainBlockGraphNode;
                node.additional_info = {
                    gen_utime: block_info.gen_utime,
                    shard: block_info.shard,
                    workchain_id: block_info.workchain_id,
                    message_ids: block_info.message_ids,
                    transaction_ids: block_info.transaction_ids,
                };
                
                const prev_block_id = block_info.prev_block_id;
                let prev_graph_node = process_prev(prev_block_id, node, next_shardchain_block_ids);
                node.prev = prev_graph_node;
                
                const prev_alt_block_id = block_info.prev_alt_block_id;
                if (prev_alt_block_id) {
                    let prev_alt_graph_node = process_prev(prev_alt_block_id, node, next_shardchain_block_ids);
                    node.prev_alt = prev_alt_graph_node;
                }
            });

            current_depth++;
        }

        // Helper functions
        function process_prev(
            prev_block_id: string, 
            current_block_graph_node: TopShardChainBlockGraphNode | MidShardChainBlockGraphNode,
            next_shardchain_block_ids: string[],
        ) {
            let prev_graph_node = blocks.get(prev_block_id) as RootShardchainBlockGraphNode | MidShardChainBlockGraphNode;
            if (prev_graph_node) {
                if (!prev_graph_node.next) {
                    // This case is for "Root" nodes
                    prev_graph_node.next = current_block_graph_node;
                } else if (!prev_graph_node.next_alt) {
                    prev_graph_node.next_alt = current_block_graph_node;
                } else {
                    throw new Error(`Triple next block for block "${prev_graph_node.id}"`);
                }
            } else {
                prev_graph_node = {
                    type: "Mid",
                    id: prev_block_id,
                    next: current_block_graph_node,
                };
                blocks.set(prev_block_id, prev_graph_node);
                next_shardchain_block_ids.push(prev_block_id);
            }
            return prev_graph_node;
        }
    }

    private async flatten_graph_and_prepare_result(): Promise<void> {
        this.result = Array.from(this.blocks!.values())
            .filter(f => f.type != "Root")
            .map(node => {
                return {
                    block_id: node.id,
                    transaction_ids: node.additional_info!.transaction_ids,
                    message_ids: node.additional_info!.message_ids,
                };
            })
            .concat({
                block_id: this.masterchain_block_info!._key,
                transaction_ids: this.masterchain_block_info!.transaction_ids,
                message_ids: this.masterchain_block_info!.message_ids,
            })
            .reduce((result, current) => {
                result.block_ids.push(current.block_id);
                result.message_ids.push(...current.message_ids!);
                result.transaction_ids.push(...current.transaction_ids!);
                return result;
            }, {
                block_ids: [] as string[],
                transaction_ids: [] as string[],
                message_ids: [] as string[],
            });

        if (RangerConfig.debug) {
            console.log(this.result);
        }
    }
}

export type AdditionalBlockInfo = {
    gen_utime: number,
    workchain_id: string,
    shard: string,
    message_ids: string[],
    transaction_ids: string[],
}

export type RootShardchainBlockGraphNode = {
    type: "Root",
    id: string,
    next?: ShardChainBlockGraphNode,
    next_alt?: ShardChainBlockGraphNode,
    additional_info?: AdditionalBlockInfo,
};

export type TopShardChainBlockGraphNode = {
    type: "Top",
    id: string,
    prev?: ShardChainBlockGraphNode,
    prev_alt?: ShardChainBlockGraphNode,
    additional_info?: AdditionalBlockInfo,
};

export type MidShardChainBlockGraphNode = {
    type: "Mid",
    id: string,
    next: ShardChainBlockGraphNode,
    next_alt?: ShardChainBlockGraphNode,
    prev?: ShardChainBlockGraphNode,
    prev_alt?: ShardChainBlockGraphNode,
    additional_info?: AdditionalBlockInfo,
};

export type ShardChainBlockGraphNode = RootShardchainBlockGraphNode | MidShardChainBlockGraphNode | TopShardChainBlockGraphNode;