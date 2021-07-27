import { MasterChainBlock, ShardChainBlock } from "./distributed-bmt-db";

export type Bmt = {
    master_block: MasterChainBlock,
    shard_blocks: ShardChainBlock[],
};