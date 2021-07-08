import { BlockWalker } from "./block-walker";
import { BMT_IDs } from "./bmt-types";
import { IBmtDb } from "./bmt-db";
import { RangerConfig } from "./ranger-config";

export class Ranger {
    readonly buffer_db: IBmtDb;
    readonly ordered_db: IBmtDb;
    readonly start_seq_no: number;

    constructor(buffer_db: IBmtDb, ordered_db: IBmtDb) {
        this.buffer_db = buffer_db;
        this.ordered_db = ordered_db;
        this.start_seq_no = RangerConfig.test_mode ? 200000 : -1;
    }

    async run() {
        await this.init_ordered_db_if_needed();
        while (true) {
            try {
                await this.run_once();
                
                if (RangerConfig.debug) {
                    console.log(await this.ordered_db.get_last_masterchain_block_seq_no());
                }
            } catch (error) {
                if (RangerConfig.debug) {
                    console.error(error);
                }

                await new Promise(resolve => setTimeout(resolve, 5000));
            } 
        }
    }

    async init_ordered_db_if_needed() {
        if (await there_is_no_masterchain_blocks_in(this.ordered_db, this.start_seq_no)) {
            await this.ordered_db.init_from(this.buffer_db, this.start_seq_no);
        }

        async function there_is_no_masterchain_blocks_in(db: IBmtDb, start_seq_no: number): Promise<boolean> {
            const masterchain_block = await db.get_first_masterchain_block_id_with_seq_no_not_less_than(start_seq_no);
            return !masterchain_block;
        }
    }

    async run_once() {
        const masterchain_block_id = await this.select_next_masterchain_block_to_process();
        const bmt_ids = await this.get_BMT_ids_tied_to_masterchain_block(masterchain_block_id);
        await this.ensure_all_messages_and_transactions_exist(bmt_ids); // blocks should exist by design of previous line
        await this.move_BMT_from_buffer_db_to_ordered_db(bmt_ids);
    }

    private async select_next_masterchain_block_to_process(): Promise<string> {
        let block_id: string | undefined;
        if (!RangerConfig.test_mode) {
            block_id = await this.buffer_db.get_first_masterchain_block_id_with_seq_no_not_less_than(this.start_seq_no);
        } else {
            const last_processed_seq_no = await this.ordered_db.get_last_masterchain_block_seq_no();
            
            if (!last_processed_seq_no || last_processed_seq_no < this.start_seq_no) {
                throw new Error("This error should be impossible (select_next_masterchain_block_to_process)");
            }

            block_id = await this.buffer_db.get_first_masterchain_block_id_with_seq_no_not_less_than(last_processed_seq_no + 1);
        }

        if (!block_id) {
            throw new Error("There is no masterblock to process");
        }

        return block_id;
    }

    private async get_BMT_ids_tied_to_masterchain_block(masterchain_block_id: string): Promise<BMT_IDs> {
        const block_walker = new BlockWalker(
            this.buffer_db,
            this.ordered_db,
            masterchain_block_id,
        );
        await block_walker.run();
        return block_walker.result!;
    }

    private async ensure_all_messages_and_transactions_exist(bmt_ids: BMT_IDs): Promise<void> {
        const buffer_db_message_ids = await this.buffer_db.get_existing_message_ids_from_id_array(bmt_ids.message_ids);
        const remaining_message_ids = bmt_ids.message_ids.filter(id => buffer_db_message_ids.indexOf(id) == -1);
        const non_existant_message_ids = await this.ordered_db.get_existing_message_ids_from_id_array(remaining_message_ids);
        if (non_existant_message_ids.length > 0) {
            throw new Error(`Not all messages are here. Not found: ${non_existant_message_ids}`);
        }

        const buffer_db_transaction_ids = await this.buffer_db.get_existing_transaction_ids_from_id_array(bmt_ids.transaction_ids);
        const remaining_transaction_ids = bmt_ids.transaction_ids.filter(id => buffer_db_transaction_ids.indexOf(id) == -1);
        const non_existant_transaction_ids = await this.ordered_db.get_existing_transaction_ids_from_id_array(remaining_transaction_ids);
        if (non_existant_transaction_ids.length > 0) {
            throw new Error(`Not all transactions are here. Not found: ${non_existant_transaction_ids}`);
        }
    }

    private async move_BMT_from_buffer_db_to_ordered_db(bmt_ids: BMT_IDs): Promise<void> {
        const bmt = await this.buffer_db.get_BMT_by_ids(bmt_ids);
        await this.ordered_db.add_BMT(bmt);
        
        if (!RangerConfig.test_mode) {
            await this.buffer_db.remove_BMT_by_ids(bmt_ids);
        }
    }
}
