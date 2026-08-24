// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

interface PiSessionsRootOptions {
  readonly environment: Readonly<Record<string, string | undefined>>;
  readonly homeDir: string;
  readonly cwd: string;
  readonly settingsSessionDir?: string;
}

function resolvePiPath(value: string, homeDir: string, cwd: string): string {
  const homeRelative = value.startsWith("~/") || value.startsWith("~\\");
  const expanded =
    value === "~" ? homeDir : homeRelative ? NodePath.join(homeDir, value.slice(2)) : value;
  return NodePath.resolve(cwd, expanded);
}

/** Mirrors Pi's session-directory precedence for catalog and usage discovery. */
export function resolvePiSessionsRoot(options: PiSessionsRootOptions): string {
  const agentDir = resolvePiPath(
    options.environment.PI_CODING_AGENT_DIR ?? NodePath.join(options.homeDir, ".pi", "agent"),
    options.homeDir,
    options.cwd,
  );
  const configured = [
    options.environment.T3_PI_SESSIONS_ROOT,
    options.environment.PI_CODING_AGENT_SESSION_DIR,
    options.settingsSessionDir,
  ].find((value): value is string => value !== undefined && value.length > 0);
  return configured === undefined
    ? NodePath.join(agentDir, "sessions")
    : resolvePiPath(configured, options.homeDir, options.cwd);
}

function readSettingsSessionDir(settingsPath: string): string | undefined {
  try {
    const settings: unknown = JSON.parse(NodeFS.readFileSync(settingsPath, "utf8"));
    if (typeof settings !== "object" || settings === null) return undefined;
    const sessionDir = (settings as Record<string, unknown>).sessionDir;
    return typeof sessionDir === "string" ? sessionDir : undefined;
  } catch {
    return undefined;
  }
}

export const defaultPiSessionsRoot = () => {
  const homeDir = NodeOS.homedir();
  const cwd = process.cwd();
  const environment = process.env;
  const agentDir = resolvePiPath(
    environment.PI_CODING_AGENT_DIR ?? NodePath.join(homeDir, ".pi", "agent"),
    homeDir,
    cwd,
  );
  const settingsSessionDir = readSettingsSessionDir(NodePath.join(agentDir, "settings.json"));
  return resolvePiSessionsRoot({
    environment,
    homeDir,
    cwd,
    ...(settingsSessionDir === undefined ? {} : { settingsSessionDir }),
  });
};
