import { BlockWalker } from "./block-walker";
import { DbWrapper, MasterChainBlockInfo } from "./db-wrapper";

export class Ranger {
    readonly unordered_db: DbWrapper;
    readonly ordered_db: DbWrapper;

    constructor(unordered_db: DbWrapper, ordered_db: DbWrapper) {
        this.unordered_db = unordered_db;
        this.ordered_db = ordered_db;
    }

    async run() {
        const unordered_max_seq_no = await this.unordered_db.get_last_masterchain_block_seqno();
        const ordered_max_seq_no = await this.ordered_db.get_last_masterchain_block_seqno();
        console.log("Unordered max seq_no: ", unordered_max_seq_no);
        console.log("Ordered max seq_no: ", ordered_max_seq_no);

        const result = await this.get_sharchain_blocks_between_masterchain_blocks(
            (await this.unordered_db.get_first_masterchain_block_id_with_seq_no_not_less_than(599996))!,
            (await this.unordered_db.get_first_masterchain_block_id_with_seq_no_not_less_than(600000))!,
            // "e8af9e89914f10e9b97167e7328cbac77e9e7f82bdb27b73c55d79fa6eb7eb98", // seq_no = 599996
            // "b4ddac6fb5b40ea143a1751d33c994dc7ba3198cbba3f3850fd2b3623accf6dc", // seq_no = 600000
        );

        console.log(result);
    }

    private async get_shardchain_blocks_for_masterchain_block(masterchain_block: MasterChainBlockInfo, previous_masterchain_block?: MasterChainBlockInfo) {
        const block_walker = new BlockWalker(
            this.unordered_db,
            masterchain_block,
            previous_masterchain_block,
        );
        await block_walker.run();
        return block_walker.result!;
    }

    private async get_sharchain_blocks_between_masterchain_blocks(left_masterchain_id: string, right_masterchain_id: string) {
        let right_masterchain_block = await this.unordered_db.get_masterchain_block_info_by_id(right_masterchain_id);
        if (!right_masterchain_block)
            throw new Error(`Masterchain block with id ${right_masterchain_id} not found`);

        let left_masterchain_block = await this.unordered_db.get_masterchain_block_info_by_id(left_masterchain_id);
        if (!left_masterchain_block)
            throw new Error(`Masterchain block with id ${left_masterchain_id} not found`);

        if (left_masterchain_block.seq_no >= right_masterchain_block.seq_no)
            throw new Error(`Expected right seq_no (${right_masterchain_block.seq_no}) to be greater than left seq_no (${left_masterchain_block.seq_no})`);

        const masterchain_blocks: MasterChainBlockInfo[] = [ right_masterchain_block ];
        while(right_masterchain_block._key != left_masterchain_block?._key) {
            const next_right_id: string = right_masterchain_block!.prev_ref.root_hash;
            right_masterchain_block = await this.unordered_db.get_masterchain_block_info_by_id(next_right_id);
            if (!right_masterchain_block)
                throw new Error(`Masterchain block with id ${next_right_id} not found`);

            masterchain_blocks.unshift(right_masterchain_block);
        }

        const result_ids: string[] = [];
        for (let i = 1; i < masterchain_blocks.length; i++){
            const shardchain_block_ids = 
                await this.get_shardchain_blocks_for_masterchain_block(masterchain_blocks[i], masterchain_blocks[i-1]);
            
            result_ids.push(...shardchain_block_ids);
            result_ids.push(masterchain_blocks[i]._key);
        }

        return result_ids;
    }
}
