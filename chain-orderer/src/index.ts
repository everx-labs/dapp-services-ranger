import fs from "fs";
import os from "os";
import path from "path";
import { ChainOrderVerifier, ChainOrderVerifierConfig } from "./chain-order-verifier";
import { ChainOrderer, ChainOrdererConfig } from "./chain-orderer";

// CAUTION: block correction via vertical blockchain wasn't considered

void (async () => {
    try { 
        const configPath = process.argv.length > 2 ? process.argv[2] : path.resolve(os.homedir(), ".tonlabs", "chain-orderer.config.json");
        const config = JSON.parse(fs.readFileSync(configPath, "utf8")) as ChainOrdererConfig | ChainOrderVerifierConfig;

        if ((config as ChainOrderVerifierConfig).only_verify_from_seq_no) {
            const verifier = await ChainOrderVerifier.create(config as ChainOrderVerifierConfig);
            await verifier.run();
        } else {
            const chain_orderer = await ChainOrderer.create(config);
            await chain_orderer.run();
        }
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
})();

