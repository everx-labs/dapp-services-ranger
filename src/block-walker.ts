import {
    DbWrapper,
    MasterChainBlockInfo
} from "./db-wrapper";

export class BlockWalker {
    static MAX_DEPTH = 10;
    readonly db: DbWrapper;
    readonly masterchain_block: MasterChainBlockInfo;
    previous_masterchain_block?: MasterChainBlockInfo;
    root_shardchain_block_ids?: ReadonlyArray<string>;
    top_shardchain_block_ids?: ReadonlyArray<string>;
    blocks?: Map<string, ShardChainBlockGraphNode>;
    result?: string[];

    constructor(db: DbWrapper, masterchain_block: MasterChainBlockInfo, previous_masterchain_block?: MasterChainBlockInfo) {
        this.db = db;
        this.masterchain_block = masterchain_block;
        this.previous_masterchain_block = previous_masterchain_block;
    }

    async run() {
        await this.check_previous_masterblock();
        await this.init_shardchain_walk_boundaries();
        await this.walk_shardchain_blocks_and_fill_graph();
        await this.flatten_graph_to_id_order();
    }

    private async check_previous_masterblock() {
        if (!this.previous_masterchain_block)
            this.previous_masterchain_block = await this.db.get_masterchain_block_info_by_id(this.masterchain_block.prev_ref.root_hash);

        if (!this.previous_masterchain_block)
            throw new Error(`Previous masterchain block with id "${this.masterchain_block.prev_ref.root_hash}" not found`);

        if (this.previous_masterchain_block._key != this.masterchain_block.prev_ref.root_hash)
            throw new Error(`Wrong previous masterchain block: expected id "${this.masterchain_block.prev_ref.root_hash}", but got "${this.previous_masterchain_block._key}"`);
    }

    private async init_shardchain_walk_boundaries() {
        this.root_shardchain_block_ids = this.previous_masterchain_block!.shards.map(sh => sh.root_hash);
        this.top_shardchain_block_ids = this.masterchain_block.shards.map(sh => sh.root_hash);
    }

    private async walk_shardchain_blocks_and_fill_graph() {
        const blocks = this.blocks = new Map<string, ShardChainBlockGraphNode>();
        
        this.root_shardchain_block_ids!.forEach(sh_id => {
            blocks.set(sh_id, {
                type: "Root",
                id: sh_id,
                next: undefined,
                next_alt: undefined,
                gen_utime: undefined,
                shard: undefined,
                workchain_id: undefined,
            });
        });

        let current_shardchain_block_ids = this.top_shardchain_block_ids!
            .filter(id => !blocks.get(id)) // skip root blocks
            .map(id => { return { id: id, depth: 0}; });

        while (true) {
            const current_block_info = current_shardchain_block_ids.shift();
            if (!current_block_info)
                break;

            if (current_block_info.depth > BlockWalker.MAX_DEPTH)
                throw new Error(`Max search depth of ${BlockWalker.MAX_DEPTH} exceeded`);

            let current_block_graph_node = blocks.get(current_block_info.id) as TopShardChainBlockGraphNode | MidShardChainBlockGraphNode;
            if (!current_block_graph_node) {
                current_block_graph_node = {
                    type: "Top",
                    id: current_block_info.id,
                    prev: undefined,
                    prev_alt: undefined,
                    gen_utime: undefined,
                    shard: undefined,
                    workchain_id: undefined,
                };
                blocks.set(current_block_info.id, current_block_graph_node);
            }

            const current_block = await this.db.get_shardchain_block_info_by_id(current_block_info.id);
            if (!current_block)
                throw new Error(`Shardchain block with id "${current_block_info.id}" not found`);

            current_block_graph_node.gen_utime = current_block.gen_utime;
            current_block_graph_node.shard = current_block.shard;
            current_block_graph_node.workchain_id = current_block.workchain_id;


            const prev_block_id = current_block.prev_ref.root_hash;
            let prev_graph_node = get_prev_graph_node_and_attach_current_as_next(prev_block_id, current_block_graph_node, current_shardchain_block_ids, current_block_info.depth);
            current_block_graph_node.prev = prev_graph_node;

            const prev_alt_block_id = current_block.prev_alt_ref.root_hash;
            if (prev_alt_block_id) {
                let prev_alt_graph_node = get_prev_graph_node_and_attach_current_as_next(prev_alt_block_id, current_block_graph_node, current_shardchain_block_ids, current_block_info.depth);
                current_block_graph_node.prev_alt = prev_alt_graph_node;
            }
        }

        // Helper functions
        function get_prev_graph_node_and_attach_current_as_next(
            prev_block_id: string, 
            current_block_graph_node: TopShardChainBlockGraphNode | MidShardChainBlockGraphNode,
            current_shardchain_block_ids: { id: string, depth: number }[],
            current_depth: number,
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
                    next_alt: undefined,
                    prev: undefined,
                    prev_alt: undefined,
                    gen_utime: undefined,
                    shard: undefined,
                    workchain_id: undefined,
                };
                blocks.set(prev_block_id, prev_graph_node);
                current_shardchain_block_ids.push({ id: prev_block_id, depth: current_depth + 1 });
            }
            return prev_graph_node;
        }
    }

    async flatten_graph_to_id_order() {
        this.result = 
            Array.from(this.blocks!.values())
                .filter(f => f.type != "Root")
                .sort((a, b) =>
                    a.gen_utime != b.gen_utime
                    ? a.gen_utime! - b.gen_utime!
                    : (a.workchain_id != b.workchain_id
                    ? a.workchain_id!.localeCompare(b.workchain_id!)
                    : a.shard!.localeCompare(b.shard!)))
                .map(node => node.id);
    }
}

export type RootShardchainBlockGraphNode = {
    type: "Root",
    id: string,
    next: ShardChainBlockGraphNode | undefined,
    next_alt: ShardChainBlockGraphNode | undefined,
    gen_utime: number | undefined,
    workchain_id: string | undefined,
    shard: string | undefined,
};

export type TopShardChainBlockGraphNode = {
    type: "Top",
    id: string,
    prev: ShardChainBlockGraphNode | undefined,
    prev_alt: ShardChainBlockGraphNode | undefined,
    gen_utime: number | undefined,
    workchain_id: string | undefined,
    shard: string | undefined,
};

export type MidShardChainBlockGraphNode = {
    type: "Mid",
    id: string,
    next: ShardChainBlockGraphNode,
    next_alt: ShardChainBlockGraphNode | undefined,
    prev: ShardChainBlockGraphNode | undefined,
    prev_alt: ShardChainBlockGraphNode | undefined,
    gen_utime: number | undefined,
    workchain_id: string | undefined,
    shard: string | undefined,
};

export type ShardChainBlockGraphNode = RootShardchainBlockGraphNode | MidShardChainBlockGraphNode | TopShardChainBlockGraphNode;