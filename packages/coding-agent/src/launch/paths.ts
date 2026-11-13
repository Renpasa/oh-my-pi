import * as fs from "node:fs/promises";
import * as path from "node:path";
import { ipcEndpoint } from "@oh-my-pi/pi-natives";
import { getDaemonRuntimeDir, getDaemonRuntimeRoot, hasFsCode, isEacces, isEisdir, isEnoent } from "@oh-my-pi/pi-utils";

/** Resolve the private runtime directory shared by omp processes in one project directory. */
export { getDaemonRuntimeDir as daemonRuntimeDir };

/** File in a broker runtime dir recording which project (or global service dir) owns the scope. */
const SCOPE_FILE = "scope.json";
/**
 * Small mutable lifecycle state (snapshot, completion bookkeeping) in
 * `<runtimeDir>/daemons/<name>/`. Rewritten on transitions, and only when its
 * serialized form changes.
 */
export const DAEMON_META_FILE = "meta.json";
/**
 * The launch spec, including its environment (often the whole parent env,
 * tens of KB). Written when the record is created or its mode changes, never
 * on lifecycle transitions. Older brokers embedded it in `meta.json`; readers
 * still accept that layout and broker recovery migrates it.
 */
export const DAEMON_SPEC_FILE = "spec.json";

/**
 * Canonicalize a project directory the same way every broker client does, so
 * hash-keyed runtime dirs and Windows pipe names agree across processes.
 * Missing paths and permission-denied lookups (EPERM/EACCES on protected
 * parent directories) resolve without realpath instead of failing.
 */
export async function canonicalProjectDir(projectDir: string): Promise<string> {
	const resolved = path.resolve(projectDir);
	try {
		return await fs.realpath(resolved);
	} catch (error) {
		if (isEnoent(error) || isEisdir(error) || isEacces(error) || hasFsCode(error, "EPERM")) return resolved;
		throw error;
	}
}

/**
 * Record the scope's canonical project directory inside its runtime dir.
 * Written by the broker at startup so out-of-process inspectors (`omp ps`)
 * can map a hash-keyed runtime dir back to its project.
 */
export async function writeDaemonScopeMeta(runtimeDir: string, projectDir: string): Promise<void> {
	await Bun.write(path.join(runtimeDir, SCOPE_FILE), JSON.stringify({ projectDir }));
}

/** Read the project directory recorded for a runtime dir; undefined when absent or malformed. */
export async function readDaemonScopeMeta(runtimeDir: string): Promise<string | undefined> {
	try {
		const raw: unknown = await Bun.file(path.join(runtimeDir, SCOPE_FILE)).json();
		if (typeof raw === "object" && raw !== null && "projectDir" in raw && typeof raw.projectDir === "string") {
			return raw.projectDir;
		}
	} catch {
		// Missing or malformed scope metadata reads as unknown.
	}
	return undefined;
}

/** One daemon record dir as stored on disk, before parsing. */
export interface StoredDaemonRecord {
	/** Decoded `meta.json`. */
	meta: object & { daemon: unknown };
	/** Decoded launch spec, from `meta.json` (legacy layout) or `spec.json`. */
	spec: unknown;
	/** The spec was embedded in `meta.json` by an older broker. */
	legacyLayout: boolean;
}

/**
 * Read a daemon record dir in either layout. Undefined when `meta.json` has no
 * daemon snapshot; throws when a file is missing or malformed.
 */
export async function readStoredDaemonRecord(dir: string): Promise<StoredDaemonRecord | undefined> {
	const meta: unknown = await Bun.file(path.join(dir, DAEMON_META_FILE)).json();
	if (typeof meta !== "object" || meta === null || !("daemon" in meta)) return undefined;
	if ("spec" in meta) return { meta, spec: meta.spec, legacyLayout: true };
	const spec: unknown = await Bun.file(path.join(dir, DAEMON_SPEC_FILE)).json();
	return { meta, spec, legacyLayout: false };
}

/** Canonicalize `target` through its deepest existing ancestor; the not-yet-created tail is appended as given. */
async function canonicalAllowingMissing(target: string): Promise<string> {
	const tail: string[] = [];
	let current = path.resolve(target);
	for (;;) {
		try {
			return path.join(await fs.realpath(current), ...tail);
		} catch (error) {
			const parent = path.dirname(current);
			if (!isEnoent(error) || parent === current) throw error;
			tail.unshift(path.basename(current));
			current = parent;
		}
	}
}

/**
 * Validate a caller-chosen broker runtime directory meant to isolate one broker from the shared
 * per-project scopes, returning the directory to use. It must be absolute, contain no `..` segment,
 * and have no symlinked component; its canonical location must be neither inside this profile's
 * daemon runtime root nor an ancestor of it. That keeps an isolated broker from joining a live
 * shared broker or sitting among the sibling scopes `pruneDeadDaemonRuntimeDirs` reclaims, however
 * the path is spelled.
 */
export async function resolvePrivateRuntimeDir(runtimeDir: string, label: string): Promise<string> {
	if (!path.isAbsolute(runtimeDir)) throw new Error(`${label} must be an absolute path`);
	if (runtimeDir.split(/[\\/]/).includes("..")) throw new Error(`${label} must not contain '..' segments`);
	const resolved = path.resolve(runtimeDir);
	const { root } = path.parse(resolved);
	let current = root;
	for (const part of resolved.slice(root.length).split(path.sep).filter(Boolean)) {
		current = path.join(current, part);
		let stat: Awaited<ReturnType<typeof fs.lstat>>;
		try {
			stat = await fs.lstat(current);
		} catch (error) {
			if (isEnoent(error)) break; // the rest does not exist yet; the broker creates it
			throw error;
		}
		if (stat.isSymbolicLink()) throw new Error(`${label} must not contain symlinked components: ${current}`);
	}
	const canonical = await canonicalAllowingMissing(resolved);
	const sharedRoot = await canonicalAllowingMissing(getDaemonRuntimeRoot());
	const inside = (parent: string, child: string): boolean => {
		const relative = path.relative(parent, child);
		const escapes = relative === ".." || relative.startsWith(`..${path.sep}`);
		return relative === "" || (!escapes && !path.isAbsolute(relative));
	};
	if (inside(sharedRoot, canonical)) {
		throw new Error(`${label} must not be inside the shared daemon runtime root ${sharedRoot}`);
	}
	if (inside(canonical, sharedRoot)) {
		throw new Error(`${label} must not contain the shared daemon runtime root ${sharedRoot}`);
	}
	return canonical;
}

/** Resolve the Unix socket or Windows named pipe used by one daemon broker scope. */
export function daemonBrokerEndpoint(runtimeDir: string): string {
	return ipcEndpoint(runtimeDir, "broker");
}
