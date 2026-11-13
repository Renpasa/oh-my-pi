/**
 * Submit one Parent Wake Transport v1 event to the daemon broker.
 */

import { Command, Flags } from "@oh-my-pi/pi-utils/cli";
import { parentWakeHelp as commandHelp } from "../cli/command-help";
import { type ParentWakeCommandArgs, runParentWakeCommand } from "../cli/parent-wake-cli";

export default class ParentWake extends Command {
	static description = commandHelp.description;

	static flags = {
		"project-dir": Flags.string({
			description: "Broker scope to connect to; defaults to the current project directory",
		}),
		"runtime-dir": Flags.string({
			description: "Override the broker's private runtime directory (advanced; for isolated scopes)",
		}),
		timeout: Flags.integer({
			description: "Milliseconds to wait for the target's acknowledgement before reporting a timeout",
		}),
	};

	static examples = [
		"cat event.json | omp parent-wake --project-dir /path/to/project",
		"cat event.json | omp parent-wake --project-dir /path/to/project --runtime-dir /path/to/runtime --timeout 60000",
	];

	async run(): Promise<void> {
		const { flags } = await this.parse(ParentWake);
		const cmd: ParentWakeCommandArgs = {
			flags: {
				projectDir: flags["project-dir"],
				runtimeDir: flags["runtime-dir"],
				timeout: flags.timeout,
			},
		};
		await runParentWakeCommand(cmd);
	}
}
