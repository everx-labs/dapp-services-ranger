const REPORT_PERIOD_MS = 1000;

export class Reporter {
    started_at = Date.now()
    last_report = Date.now();

    report_step(chain_order: string): void {
        if (Date.now() - this.last_report >= REPORT_PERIOD_MS) {
            this.last_report = Date.now();
            console.log(`${this.last_report - this.started_at} ms, t.chain_order: ${chain_order}`);
        }
    }

    report_finish(chain_order: string): void {
        console.log(`Finished at t.chain_order ${chain_order}`);
    }
}
