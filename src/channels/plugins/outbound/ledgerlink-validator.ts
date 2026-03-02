import { spawnSync } from "child_process";
import path from "path";

type ValidateResult = {
  status: "OK" | "BLOCK";
  reasons: string[];
  required_action: string;
};

function getPythonPath(): string {
  // Portable venv inside THIS install root
  return path.resolve(process.cwd(), ".venv", "Scripts", "python.exe");
}

function getValidatorPath(): string {
  // Per-install system folder inside THIS install root
  return path.resolve(process.cwd(), ".ledgerlink_system", "validator.py");
}

export function validateLedgerLinkOutputOrThrow(text: string, channel: string): void {
  const trimmed = (text ?? "").trim();
  if (!trimmed) return;

  const py = getPythonPath();
  const validator = getValidatorPath();

  const input = JSON.stringify({ text: trimmed, channel });

  const res = spawnSync(py, [validator], {
    input,
    encoding: "utf-8",
    windowsHide: true,
    timeout: 15_000,
  });

  // Fail closed
  if (res.error) {
    throw new Error(`LedgerLink validator execution failed: ${res.error.message}`);
  }

  let parsed: ValidateResult;
  try {
    parsed = JSON.parse(res.stdout || "");
  } catch {
    throw new Error(`LedgerLink validator returned non-JSON output: ${res.stdout || res.stderr}`);
  }

  if (parsed.status !== "OK") {
    const why = (parsed.reasons || []).join(" | ");
    throw new Error(
      `LedgerLink BLOCKED outbound message (${channel}). ${why} Required: ${parsed.required_action}`,
    );
  }
}
