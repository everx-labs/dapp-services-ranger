import { Config } from "arangojs/connection";
import { Block, BmtDb } from "./bmt-db";
import { ChainRange, ChainRangesDb } from "./chain-ranges-db";
import { ChainRangesVerificationDb } from "./chain-ranges-verification-db";
import { Reporter } from "./reporter";

export class Ranger {
    readonly bmt_db: BmtDb;
    readonly chain_ranges_verification_db: ChainRangesVerificationDb;
    readonly chain_ranges_db: ChainRangesDb;
    readonly database_polling_period: number;
    readonly max_ids_per_request: number;

    constructor(config: RangerConfig) {
        this.bmt_db = new BmtDb(config.bmt_database);
        this.chain_ranges_verification_db = new ChainRangesVerificationDb(config.chain_ranges_verification_database);
        this.chain_ranges_db = new ChainRangesDb(config.chain_ranges_database);
        this.database_polling_period = config.database_polling_period ?? 1000;
        this.max_ids_per_request = config.max_ids_per_request ?? 10000;
    }

    async run(): Promise<void> {
        const summary = await this.chain_ranges_verification_db.get_summary();
        let last_verified_master_seq_no = summary.last_verified_master_seq_no;
        const reporter = new Reporter();

        while (true) {
            const chain_range = await this.chain_ranges_db.get_range_by_mc_seq_no(last_verified_master_seq_no + 1);
            if (!chain_range) {
                await delay(this.database_polling_period);
                continue;
            }
            await this.ensure_chain_range_is_in_bmt_db(chain_range);
            await this.chain_ranges_verification_db.update_verifyed_boundary(last_verified_master_seq_no + 1);
            last_verified_master_seq_no++;
            reporter.report(last_verified_master_seq_no);
        }
    }

    private async ensure_chain_range_is_in_bmt_db(chain_range: ChainRange): Promise<void> {
        let block_ids_to_check = [chain_range.master_block.id, ...chain_range.shard_blocks_ids];
        let transaction_ids_to_check = [] as string[];
        let message_ids_to_check = [] as string[];

        while (true) {
            const delayPromise = delay(this.database_polling_period);
            if (block_ids_to_check.length > 0) {
                const blocks = await this.bmt_db.find_blocks_by_ids(block_ids_to_check);
                this.ensure_chain_range_on_blocks(blocks);
                transaction_ids_to_check = transaction_ids_to_check.concat(...blocks.map(b => b.transaction_ids));
                message_ids_to_check = message_ids_to_check.concat(...blocks.map(b => b.message_ids));
                block_ids_to_check = block_ids_to_check.filter(id => !blocks.find(b => b.id == id));
            }
            if (transaction_ids_to_check.length > 0 || message_ids_to_check.length > 0) {
                const mt_ids = await this.find_existing_mt_ids(message_ids_to_check, transaction_ids_to_check);
                const transaction_ids = new Set(mt_ids.transactions);
                const message_ids = new Set(mt_ids.messages);
                transaction_ids_to_check = transaction_ids_to_check.filter(id => !transaction_ids.has(id));
                message_ids_to_check = message_ids_to_check.filter(id => !message_ids.has(id));
            }

            if (block_ids_to_check.length == 0 && transaction_ids_to_check.length == 0 && message_ids_to_check.length == 0) {
                break;
            } else {
                await delayPromise;
            }
        }
    }

    private ensure_chain_range_on_blocks(blocks: Block[]): void {
        const blocks_without_co = blocks.filter(b => !b.chain_order);
        if (blocks_without_co.length > 0) {
            throw new Error(`There is no chain range on blocks: ${blocks_without_co.map(b => b.id).join(", ")}`);
        }
    }

    private async find_existing_mt_ids(messages: string[], transactions: string[]): Promise<{ transactions: string[], messages: string[]}> {
        const result = {
            transactions: [] as string[],
            messages: [] as string[]
        };
        let m_start_index = 0;
        let t_start_index = 0;
        while (m_start_index < messages.length && t_start_index < transactions.length) {
            const m_left = Math.max(0, messages.length - m_start_index);
            const t_left = Math.max(0, transactions.length - t_start_index);
            const m_count_to_check = this.max_ids_per_request - Math.min(Math.ceil(this.max_ids_per_request / 2), t_left);
            const t_count_to_check = this.max_ids_per_request - Math.min(Math.floor(this.max_ids_per_request / 2), m_left);
            const m_ids_to_check = messages.slice(m_start_index, m_start_index + m_count_to_check);
            const t_ids_to_check = transactions.slice(t_start_index, t_start_index + t_count_to_check);

            const result_part = await this.bmt_db.find_existing_mt_ids(m_ids_to_check, t_ids_to_check);
            result.messages = result.messages.concat(result_part.messages);
            result.transactions = result.transactions.concat(result_part.transactions);

            m_start_index += m_count_to_check;
            t_start_index += t_count_to_check;
        }

        return result;
    }
}

export type RangerConfig = {
    bmt_database: Config,
    chain_ranges_database: Config,
    chain_ranges_verification_database: Config,
    database_polling_period: number | undefined,
    max_ids_per_request: number | undefined
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setInterval(resolve, ms));
}
