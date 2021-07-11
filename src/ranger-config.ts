import { Config } from "arangojs/connection";
import fs from "fs";

export class RangerConfig {
    static test_mode = true;
    static debug = true;
    private static db?: Config;

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

        if (source.db === undefined || typeof source.db !== "object") {
            throw new Error("Database config not provided properly");
        }

        this.db = source.db;
    }

    static loadFromFile(configPath: string) {
        const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
        this.initFrom(config);
    }

    static get_db_config(): Config {
        if (!this.db) {
            throw new Error("There is no database config provided");
        }
        return this.db;
    }
}