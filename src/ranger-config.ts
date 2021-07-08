import { Config } from "arangojs/connection";
import fs from "fs";

export class RangerConfig {
    static test_mode = true;
    static debug = true;
    private static buffer_db?: Config;
    private static ordered_db?: Config;

    static initFrom(source: any) {
        if (typeof source === "string") {
            source = JSON.parse(source);
        }

        if (!source) {
            throw new Error("There is no config source provided");
        }

        if (source.test_mode !== undefined) {
            this.test_mode = source.test_mode;
        }

        if (source.debug !== undefined) {
            this.debug = source.debug;
        }

        if (source.buffer_db === undefined || typeof source.buffer_db !== "object" ||
            source.ordered_db === undefined || typeof source.ordered_db !== "object") {
            throw new Error("Buffer or ordered db config not provided properly");
        }

        this.buffer_db = source.buffer_db;
        this.ordered_db = source.ordered_db;
    }

    static loadFromFile(configPath: string) {
        const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
        this.initFrom(config);
    }

    static get_buffer_db_config(): Config {
        if (!this.buffer_db) {
            throw new Error("There is no buffer db config provided");
        }
        return this.buffer_db;
    }

    static get_ordered_db_config(): Config {
        if (!this.ordered_db) {
            throw new Error("There is no ordered db config provided");
        }
        return this.ordered_db;
    }
}