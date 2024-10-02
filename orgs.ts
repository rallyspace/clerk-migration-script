import { config } from "dotenv";
config();

import * as fs from "fs";
import * as z from "zod";
import clerkClient from "@clerk/clerk-sdk-node";
import ora, { Ora } from "ora";

const SECRET_KEY = process.env.CLERK_SECRET_KEY;
const DELAY = parseInt(process.env.DELAY_MS ?? `1_000`);
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY_MS ?? `10_000`);
const IMPORT_TO_DEV = process.env.IMPORT_TO_DEV_INSTANCE ?? "false";
const OFFSET = parseInt(process.env.OFFSET ?? `0`);

if (!SECRET_KEY) {
	throw new Error(
		"CLERK_SECRET_KEY is required. Please copy .env.example to .env and add your key."
	);
}

if (SECRET_KEY.split("_")[1] !== "live" && IMPORT_TO_DEV === "false") {
	throw new Error(
		"The Clerk Secret Key provided is for a development instance. Development instances are limited to 500 users and do not share their userbase with production instances. If you want to import users to your development instance, please set 'IMPORT_TO_DEV_INSTANCE' in your .env to 'true'."
	);
}

const orgSchema = z.object({
  // The Clerk User ID of the User that will own this Organization
  createdBy: z.string(),
	/** The ID of the organization as used in your external systems or your previous authentication solution. Must be unique across your instance. */
	externalId: z.string().optional(),
	/** Email address to set as User's primary email address. */
	name: z.string(),
	/** Metadata saved on the organization, that is visible to both your Frontend and Backend APIs */
	publicMetadata: z.record(z.string(), z.unknown()).optional(),
	/** Metadata saved on the organization, that is only visible to your Backend APIs */
	privateMetadata: z.record(z.string(), z.unknown()).optional(),
	/** Email address to set as User's primary email address. */
	slug: z.string().optional(),
});

type Org = z.infer<typeof orgSchema>;

const createOrg = (orgData: Org) =>
  clerkClient.organizations.createOrganization({
    createdBy: orgData.createdBy,
    name: orgData.name,
    privateMetadata: { ...(orgData.privateMetadata || {}), externalId: orgData.externalId },
    publicMetadata: orgData.publicMetadata,
  });

const now = new Date().toISOString().split(".")[0]; // YYYY-MM-DDTHH:mm:ss
function appendLog(payload: any) {
	fs.appendFileSync(
		`./migration-log-${now}.json`,
		`\n${JSON.stringify(payload, null, 2)}`
	);
}

let migrated = 0;
let alreadyExists = 0;

async function processOrgToClerk(orgData: Org, spinner: Ora) {
	const txt = spinner.text;
	try {
		const parsedorgData = orgSchema.safeParse(orgData);
		if (!parsedorgData.success) {
			throw parsedorgData.error;
		}
		await createOrg(parsedorgData.data);

		migrated++;
	} catch (error) {
		if (error.status === 422) {
			appendLog({ userId: orgData.userId, ...error });
			alreadyExists++;
			return;
		}

		// Keep cooldown in case rate limit is reached as a fallback if the thread blocking fails
		if (error.status === 429) {
			spinner.text = `${txt} - rate limit reached, waiting for ${RETRY_DELAY} ms`;
			await rateLimitCooldown();
			spinner.text = txt;
			return processOrgToClerk(orgData, spinner);
		}

		appendLog({ externalId: orgData.externalId, ...error });
	}
}

async function cooldown() {
	await new Promise((r) => setTimeout(r, DELAY));
}

async function rateLimitCooldown() {
	await new Promise((r) => setTimeout(r, RETRY_DELAY));
}

async function main() {
	console.log(`Clerk Organization Migration Utility`);

	const inputFileName = process.argv[2] ?? "orgs.json";

	console.log(`Fetching orgs from ${inputFileName}`);

	const parsedOrgData: any[] = JSON.parse(
		fs.readFileSync(inputFileName, "utf-8")
	);
	const offsetOrgs = parsedOrgData.slice(OFFSET);
	console.log(
		`users.json found and parsed, attempting migration with an offset of ${OFFSET}`
	);

	let i = 0;
	const spinner = ora(`Migrating organizations`).start();

	for (const orgData of offsetOrgs) {
		spinner.text = `Migrating org ${i}/${offsetOrgs.length}, cooldown`;
		await cooldown();
		i++;
		spinner.text = `Migrating org ${i}/${offsetOrgs.length}`;
		await processOrgToClerk(orgData, spinner);
	}

	spinner.succeed(`Migration complete`);
	return;
}

main().then(() => {
	console.log(`${migrated} organization(s) migrated`);
	console.log(`${alreadyExists} organization(s) failed to upload`);
});
