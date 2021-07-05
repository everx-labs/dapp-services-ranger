import fs from "fs";
import os from "os";
import path from "path";
import { DbWrapper } from "./db-wrapper";
import { Ranger } from "./ranger";

// CAUTION: block correction via vertical blockchain wasn't considered

(async () => {
    try {
        const configPath = path.resolve(os.homedir(), ".tonlabs", "ranger.config.json");
        const config = JSON.parse(fs.readFileSync(configPath, "utf8"));

        const unordered_db = new DbWrapper(config.unordered_db_config);
        const ordered_db = new DbWrapper(config.ordered_db_config);
        const ranger = new Ranger(unordered_db, ordered_db);

        await ranger.run();
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
})();

