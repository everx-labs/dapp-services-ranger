import { BlockWalker } from "./block-walker";
import { BMT_IDs } from "./bmt-types";
import { IBmtDb } from "./bmt-db";
import { RangerConfig } from "./ranger-config";

export class Ranger {
    readonly buffer_db: IBmtDb;
    last_checked_seq_no: number = -1;

    constructor(buffer_db: IBmtDb) {
        this.buffer_db = buffer_db;
    }

    async run() {
        this.get_last_checked_seq_no();
        while (true) {
            try {
                await this.run_once();
            } catch (error) {
                if (RangerConfig.debug) {
                    console.error(error);
                }

                await new Promise(resolve => setTimeout(resolve, 5000));
            } 
        }
    }

    async run_once() {
        const masterchain_block_id = await this.get_next_masterchain_block_id_to_process();
        const bmt_ids = await this.get_BMT_ids_tied_to_masterchain_block(masterchain_block_id);
        await this.ensure_all_messages_and_transactions_exist(bmt_ids); // blocks should exist by design of previous line
        await this.increment_last_checked_seq_no_and_save();
    }

    private async get_last_checked_seq_no(): Promise<void> {
        // TODO: implement storage
        this.last_checked_seq_no = RangerConfig.test_mode ? 200000 : -1;
    }

    private async increment_last_checked_seq_no_and_save(): Promise<void> {
        this.last_checked_seq_no++;
        // TODO: implement storage
    }

    private async get_next_masterchain_block_id_to_process(): Promise<string> {
        const block_id = await this.buffer_db.get_first_masterchain_block_id_with_seq_no_not_less_than(this.last_checked_seq_no + 1);

        if (!block_id) {
            throw new Error("There is no masterblock to process");
        }

        return block_id;
    }

    private async get_BMT_ids_tied_to_masterchain_block(masterchain_block_id: string): Promise<BMT_IDs> {
        const block_walker = new BlockWalker(
            this.buffer_db,
            masterchain_block_id,
        );
        return await block_walker.run();
    }

    private async ensure_all_messages_and_transactions_exist(bmt_ids: BMT_IDs): Promise<void> {
        const buffer_db_message_ids = await this.buffer_db.get_existing_message_ids_from_id_array(bmt_ids.message_ids);
        const non_existant_message_ids = bmt_ids.message_ids.filter(id => buffer_db_message_ids.indexOf(id) == -1);
        if (non_existant_message_ids.length > 0) {
            throw new Error(`Not all messages are here. Not found: ${non_existant_message_ids}`);
        }

        const buffer_db_transaction_ids = await this.buffer_db.get_existing_transaction_ids_from_id_array(bmt_ids.transaction_ids);
        const non_existant_transaction_ids = bmt_ids.transaction_ids.filter(id => buffer_db_transaction_ids.indexOf(id) == -1);
        if (non_existant_transaction_ids.length > 0) {
            throw new Error(`Not all transactions are here. Not found: ${non_existant_transaction_ids}`);
        }
    }
}
