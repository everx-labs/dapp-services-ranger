import { aql, Database } from "arangojs";
import { Config } from "arangojs/connection";

export class BmtDb {
    readonly config: Config;
    readonly arango_db: Database;
    summary: DbSummary;

    private constructor(config: Config) {
        this.config = config;
        this.arango_db = new Database(config);
        this.summary = {
            min_time: -1,
            max_time: -1,
            min_mc_seq_no: null,
            max_mc_seq_no: null,
        }
    }

    static async create(config: Config): Promise<BmtDb> {
        const db = new BmtDb(config);
        await db.update_summary();
        return db;
    }

    async update_summary(): Promise<void> {
        const cursor = await this.arango_db.query(aql`
            RETURN {
                min_time: (
                    FOR b IN blocks
                        SORT b.gen_utime ASC
                        LIMIT 1
                        RETURN b.gen_utime)[0],
                max_time: (
                    FOR b IN blocks
                        SORT b.gen_utime DESC
                        LIMIT 1
                        RETURN b.gen_utime)[0],
                min_mc_seq_no: (
                    FOR b IN blocks
                        FILTER b.workchain_id == -1
                        SORT b.seq_no ASC
                        LIMIT 1
                        RETURN b.seq_no)[0],
                max_mc_seq_no: (
                    FOR b IN blocks
                        FILTER b.workchain_id == -1
                        SORT b.seq_no DESC
                        LIMIT 1
                        RETURN b.seq_no)[0],
            }
        `);
        this.summary = await cursor.next() as DbSummary;

        if (!this.summary.min_time || !this.summary.max_time) {
            throw new Error(`Invalid database ${this.arango_db.name}: there is no blocks`);
        }
    }

    async get_masterchain_block_by_seq_no(seq_no: number): Promise<MasterChainBlock> {
        const cursor = await this.arango_db.query(`
            FOR b IN blocks
                FILTER b.workchain_id == -1 && b.seq_no == ${seq_no}
                RETURN { 
                    "id": b._key,
                    "chain_order": b.chain_order,
                    "gen_utime": b.gen_utime,
                    "seq_no": b.seq_no,
                    "prev_block_id": b.prev_ref.root_hash,
                    "shard_block_ids": (
                        FOR sh IN b.master.shard_hashes
                        RETURN sh.descr.root_hash
                    ),
                    "transactions": (
                        FOR a_b IN b.account_blocks || []
                            FOR t IN a_b.transactions
                            RETURN {
                                "id": t.transaction_id,
                                "lt": t.lt,
                                "account_addr": a_b.account_addr,
                            }),
                }
        `);
        const block = await cursor.next() as MasterChainBlock | null;

        if (!block) {
            throw new Error(`Masterblock with seq_no ${seq_no} not found`);
        }

        return block;
    }

    async find_shardchain_blocks_by_ids(ids: string[]): Promise<ShardChainBlock[]> {
        const cursor = await this.arango_db.query(aql`
            FOR b IN blocks
                FILTER b._key IN ${ids} && b.workchain_id != -1
                RETURN { 
                    "id": b._key,
                    "chain_order": b.chain_order,
                    "gen_utime": b.gen_utime,
                    "workchain_id": b.workchain_id,
                    "seq_no": b.seq_no,
                    "shard": b.shard,
                    "prev_block_id": b.prev_ref.root_hash,
                    "prev_alt_block_id": b.prev_alt_ref.root_hash,
                    "transactions": (
                        FOR a_b IN b.account_blocks || []
                            FOR t IN a_b.transactions
                            RETURN {
                                "id": t.transaction_id,
                                "lt": t.lt,
                                "account_addr": a_b.account_addr,
                            }),
                }
        `);
        const blocks = await cursor.all() as ShardChainBlock[];
        return blocks;
    }

    async set_transaction_chain_orders(chain_orders: ChainOrderedTransaction[]): Promise<void> {
        const cursor = await this.arango_db.query(aql`
            FOR c_o IN ${chain_orders}
                UPDATE c_o.id 
                WITH { "chain_order": c_o.chain_order } 
                IN transactions
                OPTIONS { waitForSync: true, ignoreErrors: true }
                RETURN OLD._key
        `);
        const found_ids = await cursor.all() as string[];
        if (found_ids.length != chain_orders.length) {
            const not_found_ids = chain_orders
                .filter(c_o => !found_ids.find(id => c_o.id == id))
                .map(c_o => c_o.id);
            throw new Error(`Transactions not found: ${not_found_ids.join(", ")}`);
        }
    }

    async set_chain_order_for_block(block: Block, chain_order: string): Promise<void> {
        await this.arango_db.query(aql`
            FOR b IN blocks
            FILTER b._key == ${block.id}
            UPDATE b._key
                WITH { "chain_order": ${chain_order} } 
                IN blocks
                OPTIONS { waitForSync: true }
        `);
    }

    async verify_block_and_transactions(block: Block): Promise<void> {
        const transaction_ids = block.transactions.map(t => t.id);
        const cursor = await this.arango_db.query(aql`
            RETURN {
                b_count: (
                    FOR b IN blocks
                    FILTER b._key == ${block.id}
                    COLLECT WITH COUNT INTO c
                    RETURN c)[0],
                t_count: (
                    FOR t IN transactions
                    FILTER t._key IN ${transaction_ids}
                    COLLECT WITH COUNT INTO c
                    RETURN c)[0]
            }
        `);
        const queryResult = await cursor.next() as {
            b_count: number,
            t_count: number,
        };
        if (queryResult.b_count != 1) {
            throw new Error(`Block with id ${block.id} not found. Time: ${block.gen_utime}.`);
        }
        if (queryResult.t_count != transaction_ids.length) {
            throw new Error(`While verifying block with id ${block.id} expected ${transaction_ids.length} transactions but got ${queryResult.t_count}. Time: ${block.gen_utime}.`);
        }
    }
}

export type DbSummary = {
    min_time: number,
    max_time: number,
    min_mc_seq_no: number | null,
    max_mc_seq_no: number | null,
}

export type Transaction = {
    id: string,
    lt: string,
    account_addr: string,
};

export type ChainOrderedTransaction = {
    id: string,
    chain_order: string,
};

export type Block = {
    id: string,
    chain_order: string | null,
    gen_utime: number,
    seq_no: number,
    transactions: Transaction[],
};

export type MasterChainBlock = Block & {
    prev_block_id: string,
    shard_block_ids: string[],
};

export type ShardChainBlock = Block & {
    workchain_id: number;
    shard: string,
    prev_block_id: string,
    prev_alt_block_id: string | null,
};
