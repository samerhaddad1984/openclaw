import { spawnSync } from "node:child_process";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { loadDotEnv } from "../infra/dotenv.js";
import { normalizeEnv } from "../infra/env.js";
import { formatUncaughtError } from "../infra/errors.js";
import { isMainModule } from "../infra/is-main.js";
import { ensureOpenClawCliOnPath } from "../infra/path-env.js";
import { assertSupportedRuntime } from "../infra/runtime-guard.js";
import { installUnhandledRejectionHandler } from "../infra/unhandled-rejections.js";
import { enableConsoleCapture } from "../logging.js";
import { getCommandPathWithRootOptions, getPrimaryCommand, hasHelpOrVersion } from "./argv.js";
import { applyCliProfileEnv, parseCliProfileArgs } from "./profile.js";
import { tryRouteCli } from "./route.js";
import { normalizeWindowsArgv } from "./windows-argv.js";

export function rewriteUpdateFlagArgv(argv: string[]): string[] {
  const index = argv.indexOf("--update");
  if (index === -1) {
    return argv;
  }

  const next = [...argv];
  next.splice(index, 1, "update");
  return next;
}

export function shouldRegisterPrimarySubcommand(argv: string[]): boolean {
  return !hasHelpOrVersion(argv);
}

export function shouldSkipPluginCommandRegistration(params: {
  argv: string[];
  primary: string | null;
  hasBuiltinPrimary: boolean;
}): boolean {
  if (params.hasBuiltinPrimary) {
    return true;
  }
  if (!params.primary) {
    return hasHelpOrVersion(params.argv);
  }
  return false;
}

export function shouldEnsureCliPath(argv: string[]): boolean {
  if (hasHelpOrVersion(argv)) {
    return false;
  }
  const [primary, secondary] = getCommandPathWithRootOptions(argv, 2);
  if (!primary) {
    return true;
  }
  if (primary === "status" || primary === "health" || primary === "sessions") {
    return false;
  }
  if (primary === "config" && (secondary === "get" || secondary === "unset")) {
    return false;
  }
  if (primary === "models" && (secondary === "list" || secondary === "status")) {
    return false;
  }
  return true;
}

function tryLedgerLinkLogRotate(): void {
  // Run once per process
  if (process.env.LEDGERLINK_ROTATED === "1") return;
  process.env.LEDGERLINK_ROTATED = "1";

  // Repo root (best effort). If not in a repo, don't crash.
  const gitTop = spawnSync("git", ["rev-parse", "--show-toplevel"], {
    encoding: "utf-8",
    windowsHide: true,
  });
  const top = (gitTop.stdout || "").trim();
  if (!top) return;

  const ps1 = path.join(top, ".ledgerlink_system", "log_rotate.ps1");

  // Don't fail startup if missing; we still want OpenClaw/CLI to run.
  const res = spawnSync(
    "powershell",
    ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ps1],
    { encoding: "utf-8", windowsHide: true, timeout: 60_000 },
  );

  // Silent success. Only warn on failure.
  if (res.status && res.status !== 0) {
    console.warn("[ledgerlink] log rotation failed:", (res.stderr || res.stdout || "").trim());
  }
}

export async function runCli(argv: string[] = process.argv) {
  let normalizedArgv = normalizeWindowsArgv(argv);
  const parsedProfile = parseCliProfileArgs(normalizedArgv);
  if (!parsedProfile.ok) {
    throw new Error(parsedProfile.error);
  }
  if (parsedProfile.profile) {
    applyCliProfileEnv({ profile: parsedProfile.profile });
  }
  normalizedArgv = parsedProfile.argv;

loadDotEnv({ quiet: true });
normalizeEnv();

// LedgerLink: run startup hygiene once per process (fast + safe)
tryLedgerLinkLogRotate();

if (shouldEnsureCliPath(normalizedArgv)) {
  ensureOpenClawCliOnPath();
}

  // Enforce the minimum supported runtime before doing any work.
  assertSupportedRuntime();

  if (await tryRouteCli(normalizedArgv)) {
    return;
  }

  // Capture all console output into structured logs while keeping stdout/stderr behavior.
  enableConsoleCapture();

  const { buildProgram } = await import("./program.js");
  const program = buildProgram();

  // Global error handlers to prevent silent crashes from unhandled rejections/exceptions.
  // These log the error and exit gracefully instead of crashing without trace.
  installUnhandledRejectionHandler();

  process.on("uncaughtException", (error) => {
    console.error("[openclaw] Uncaught exception:", formatUncaughtError(error));
    process.exit(1);
  });

  const parseArgv = rewriteUpdateFlagArgv(normalizedArgv);
  // Register the primary command (builtin or subcli) so help and command parsing
  // are correct even with lazy command registration.
  const primary = getPrimaryCommand(parseArgv);
  if (primary) {
    const { getProgramContext } = await import("./program/program-context.js");
    const ctx = getProgramContext(program);
    if (ctx) {
      const { registerCoreCliByName } = await import("./program/command-registry.js");
      await registerCoreCliByName(program, ctx, primary, parseArgv);
    }
    const { registerSubCliByName } = await import("./program/register.subclis.js");
    await registerSubCliByName(program, primary);
  }

  const hasBuiltinPrimary =
    primary !== null && program.commands.some((command) => command.name() === primary);
  const shouldSkipPluginRegistration = shouldSkipPluginCommandRegistration({
    argv: parseArgv,
    primary,
    hasBuiltinPrimary,
  });
  if (!shouldSkipPluginRegistration) {
    // Register plugin CLI commands before parsing
    const { registerPluginCliCommands } = await import("../plugins/cli.js");
    const { loadConfig } = await import("../config/config.js");
    registerPluginCliCommands(program, loadConfig());
  }

  await program.parseAsync(parseArgv);
}

export function isCliMainModule(): boolean {
  return isMainModule({ currentFile: fileURLToPath(import.meta.url) });
}