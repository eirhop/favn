import {appendFileSync, mkdirSync} from "node:fs";
import {join} from "node:path";

export class Evidence {
  constructor(directory, suite) {
    this.directory = directory;
    this.suite = suite;
    mkdirSync(directory, {recursive: true});
    this.path = join(directory, "assertions.jsonl");
  }

  record(id, outcome, details = {}) {
    const entry = {
      schema_version: 1,
      assertion_id: id,
      suite: this.suite,
      outcome,
      observed_at: new Date().toISOString(),
      details: redact(details)
    };
    appendFileSync(this.path, `${JSON.stringify(entry)}\n`, {mode: 0o600});
  }
}

function redact(value) {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;

  return Object.fromEntries(
    Object.entries(value).map(([key, child]) => {
      if (/password|token|cookie|authorization|secret/i.test(key)) {
        return [key, "[REDACTED]"];
      }
      return [key, redact(child)];
    })
  );
}
