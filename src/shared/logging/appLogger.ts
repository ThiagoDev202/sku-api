type LogLevel = "info" | "error";

type LogContext = {
  runId?: string;
  cid?: string;
  seq?: number;
  sku?: string;
  statusCode?: number;
  port?: number;
  concurrency?: number;
  total?: number;
  errorCode?: string;
};

export class AppLogger {
  info(event: string, context: LogContext = {}): void {
    this.write("info", event, context);
  }

  error(event: string, context: LogContext = {}): void {
    this.write("error", event, context);
  }

  private write(level: LogLevel, event: string, context: LogContext): void {
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      event,
      ...context
    };
    process.stdout.write(`${JSON.stringify(entry)}\n`);
  }
}
