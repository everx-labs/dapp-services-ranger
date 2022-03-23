import { Config } from "arangojs/connection";

import { SrcChainOrderedEntity } from "./database/bmt-db";
import { DistributedBmtDb } from "./database/distributed-bmt-db";
import { Reporter } from "./reporter";
import { toU64String } from "./u64string";

export class ChainOrderer {
    readonly db_set: DistributedBmtDb;
    readonly chain_order_to_start_from: string;

    private constructor(db_set: DistributedBmtDb, chain_order_to_start_from: string) {
        this.db_set = db_set;
        this.chain_order_to_start_from = chain_order_to_start_from;
    }

    static async create(config: ChainOrdererConfig): Promise<ChainOrderer> {
        const db_set = await DistributedBmtDb.create(config.bmt_databases);
        return new ChainOrderer(db_set, config.chain_order_to_start_from);
    }

    async run(): Promise<void> {
        const max_transactions = 1000;
        const reporter = new Reporter();
        let min_chain_order = this.chain_order_to_start_from;
        let transactions = await this.db_set.get_transactions(min_chain_order, max_transactions);
        while (transactions.length > 0) {
            reporter.report_step(min_chain_order);
            const dst_msgs = transactions
                .filter(t => t.in_msg)
                .map(t => ({id: required(t.in_msg), dst_chain_order: `${required(t.chain_order)}00`}));
            const src_msgs = transactions
                .filter(t => t.out_msgs)
                .map(t => required(t.out_msgs)
                    .map((msg,i) => ({id: msg, src_chain_order: `${required(t.chain_order)}${toU64String(i+1)}`}))
                )
                .reduce((acc, v) => { acc.push(...v); return acc; }, [] as SrcChainOrderedEntity[]);
            await this.db_set.set_messages_chain_orders(src_msgs, dst_msgs);
            min_chain_order = transactions.map(t => required(t.chain_order)).reduce((acc, v) => (acc < v) ? acc : v, "g");
            transactions = await this.db_set.get_transactions(min_chain_order, max_transactions);
        }
        await this.db_set.ensure_messages_chain_order_indexes();
        reporter.report_finish(min_chain_order);
    }
}

export type ChainOrdererConfig = {
    bmt_databases: Config[],
    chain_order_to_start_from: string, // use "g"
}

function required<T>(value: T | undefined | null): T {
    if (value === undefined || value === null) {
        throw new Error("Value is required");
    }
    return value;
}
