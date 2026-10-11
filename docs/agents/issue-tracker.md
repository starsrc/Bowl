# Issue tracker: Linear

Issues and specs for this repo live as Linear issues. Use Linear MCP tools for all operations.

## Workspace Context

- **Team**: `StarSrc` (Key: `STARSRC`, ID: `8cc87de1-eb63-4863-ba60-1c701163997f`)
- **Project**: `Bowl` (Identifier: `P-STARSRC-2`, UUID: `4f644357-8dfa-4df8-9c11-7c0ae127e808`)
- **Assignee**: `me` / `fan-x-ing` (ID: `6761d517-a667-4dee-9f44-6c441aba4d96`)
- **Milestones**:
  - `M1: Disk & Storage Services` (ID: `b1aa3a89-c6ed-495e-b619-0fc70a791c96`)
  - `M2: Chunking & S3 Object Storage Engine` (ID: `bbc56709-9989-4dcf-947d-26a6c4aa9f4f`)

## Mandatory Constraints (Rule 3)

When creating or saving issues, always bind:
1. `team`: `"StarSrc"`
2. `project`: `"Bowl"`
3. `assignee`: `"me"`
4. `priority`: Set an explicit priority (`1` = Urgent, `2` = High, `3` = Medium, `4` = Low)
5. `milestone`: Target Milestone (e.g. `"M1: Disk & Storage Services"` or `"M2: Chunking & S3 Object Storage Engine"`)

## Conventions

- **Create an issue**: Call Linear MCP tool `save_issue` with `team: "StarSrc"`, `project: "Bowl"`, `title: "..."`, `description: "..."`, `assignee: "me"`, `priority: <1-4>`, `milestone: "..."`.
- **Read an issue**: Call Linear MCP tool `get_issue` with issue ID/identifier (e.g. `STARSRC-123`).
- **List issues**: Call Linear MCP tool `list_issues` filtered by `project: "Bowl"` or `team: "StarSrc"`.
- **Make an issue a sub-issue of a parent**: In `save_issue`, set `parentId: "<parent-id-or-identifier>"`.
- **Comment on an issue**: Call Linear MCP tool `save_comment` with `issueId: "<id>"` and `body: "..."`.
- **Apply / remove labels**: Call Linear MCP tool `save_issue` with `addLabels: ["..."]` or `removeLabels: ["..."]`.
- **Change status / Close**: In `save_issue`, set `state: "Done"` (or `"Canceled"`).

## Pull requests as a triage surface

**PRs as a request surface: no.** *(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)*

GitHub repository remote is `https://github.com/starsrc/Bowl.git`. Pull requests are managed via GitHub/`gh` CLI and the `pr` skill.

## When a skill says "publish to the issue tracker"

Create a Linear issue with `team: "StarSrc"`, `project: "Bowl"`, `assignee: "me"`, target `priority`, and `milestone`.

## When a skill says "fetch the relevant ticket"

Read it using Linear MCP tool `get_issue` or `list_issues`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single Linear issue with **child** issues as tickets.

- **Map**: A single issue with label `wayfinder:map`, holding Destination, Notes, Decisions so far, and Not yet specified sections.
- **Child ticket**: A Linear issue linked to the map via `parentId: "<map-id>"`. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`).
- **Blocking**: Linear's native dependency edges. Set `blockedBy: ["<blocker-issue-id>"]` or `blocks: ["<blocked-issue-id>"]` in `save_issue`.
- **Frontier query**: Query child issues of the map, filter for status not completed, with no unresolved `blockedBy` dependencies and unassigned.
- **Claim**: Call `save_issue` with `assignee: "me"`.
- **Resolve**: Post resolution via `save_comment`, update state to `"Done"`, and update map's "Decisions so far".
