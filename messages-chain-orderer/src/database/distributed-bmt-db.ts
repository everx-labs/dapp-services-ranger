import { Config } from "arangojs/connection";
import { 
    BmtDb, 
    SrcChainOrderedEntity,
    DstChainOrderedEntity,
    Transaction,
} from "./bmt-db";

export class DistributedBmtDb {
    readonly databases: BmtDb[];
    last_refresh = Date.now();

    private constructor(databases: BmtDb[]) {
        this.databases = [...databases];
        this.databases.sort(db => db.summary.min_time);

        this.databases.reduce((prev_time, db) => {
            if (prev_time >= db.summary.min_time) {
                throw new Error(`Time range intersection of databases is not supported,` + 
                    `but occured at ${prev_time}. One of databases is ${db.arango_db.name}`);
            }

            return db.summary.max_time;
        }, -1);

        this.databases.reverse();
    }

    static async create(config: Config[]): Promise<DistributedBmtDb> {
        const databases = await Promise.all(
            config.map(db_config => BmtDb.create(db_config)));

        return new DistributedBmtDb(databases);
    }

    async get_transactions(chain_order_lt: string, max_count: number): Promise<Transaction[]> {
        const result = [] as Transaction[];
        for (const database of this.databases) {
            if (database.summary.min_chain_order > chain_order_lt) {
                continue;
            }
            result.push(...result.concat(await database.get_transactions(chain_order_lt, max_count - result.length)));
            if (result.length > max_count / 2) {
                return result;
            }
        }
        return result;
    }

    async set_messages_chain_orders(messages_src: SrcChainOrderedEntity[], messages_dst: DstChainOrderedEntity[]): Promise<void> {
        const max_chain_order = messages_src.map(m => m.src_chain_order).concat(messages_dst.map(m => m.dst_chain_order))
            .reduce((prev, v) => (v > prev) ? v : prev, "0");

        for (const database of this.databases) {
            if (database.summary.min_chain_order > max_chain_order) {
                continue;
            }
            const result = await database.set_messages_chain_orders(messages_src, messages_dst);
            if (result.messages_src.length == messages_src.length &&
                result.messages_dst.length == messages_dst.length) {
                return;
            }

            messages_src = messages_src
                .filter(c_o => !result.messages_src.find(id => c_o.id == id));
            messages_dst = messages_dst
                .filter(c_o => !result.messages_src.find(id => c_o.id == id));
        }
        throw new Error(`Messages not found: ${messages_src.map(m => m.id).concat(messages_dst.map(m => m.id)).join(", ")}`);
    }

    async refresh_databases(): Promise<void> {
        this.last_refresh = Date.now();
        await Promise.all(
            this.databases.map(db => db.update_summary())
        );

        this.databases.sort(db => db.summary.min_time);

        this.databases.reduce((prev_time, db) => {
            if (prev_time >= db.summary.min_time) {
                throw new Error(`Time range intersection of databases is not supported,` + 
                    `but occured at ${prev_time}. One of databases is ${db.arango_db.name}`);
            }

            return db.summary.max_time;
        }, -1);
    }

    async ensure_messages_chain_order_indexes(): Promise<void> {
        await Promise.all(
            this.databases.map(db => db.ensure_messages_chain_order_indexes())
        );
    }
}
