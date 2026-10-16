# Issue Tracker: Linear

Issues for this repo are tracked in **Linear** via the `ACE_linear` MCP tools.

## Project Details & Required Metadata

创建 Issue 时必须同时绑定 `project`、`assignee`、`priority` 和 `milestone`：

- **Workspace / Team**: `StarSrc` (`STARSRC`, UUID: `8cc87de1-eb63-4863-ba60-1c701163997f`)
- **Project**: `Bowl` (`P-STARSRC-2`, UUID: `4f644357-8dfa-4df8-9c11-7c0ae127e808`)
- **Default Assignee**: `fan-x-ing` (UUID: `6761d517-a667-4dee-9f44-6c441aba4d96`)
- **Milestones**:
  - `M1: Disk & Storage Services`: `b1aa3a89-c6ed-495e-b619-0fc70a791c96`
  - `M2: Chunking & S3 Object Storage Engine`: `bbc56709-9989-4dcf-947d-26a6c4aa9f4f`
- **Priority**: `1` (Urgent), `2` (High), `3` (Normal), `4` (Low)

## Agent Operations (`ACE_linear` Exact Parameter Signatures)

- **Create / Update issue (`save_issue`)**:
  - 参数名使用 `team`、`project`、`assignee`、`milestone`、`priority`、`state`、`labels`（数组）、`parentId`、`blockedBy`（数组）。**切勿**使用 `teamId`、`stateId`、`status` 或 `labelIds`。
  - 示例：`{"team": "8cc87de1-eb63-4863-ba60-1c701163997f", "project": "4f644357-8dfa-4df8-9c11-7c0ae127e808", "assignee": "6761d517-a667-4dee-9f44-6c441aba4d96", "milestone": "b1aa3a89-c6ed-495e-b619-0fc70a791c96", "priority": 2, "title": "...", "description": "..."}`
- **Get issue (`get_issue`)**:
  - 参数名为 `id`（如 `{"id": "STARSRC-165"}`），**切勿**传 `issueId`。
- **List milestones (`list_milestones`)**:
  - 直接调用 `list_milestones({"project": "4f644357-8dfa-4df8-9c11-7c0ae127e808"})`。**切勿**对 `list_projects` 传 `includeMilestones: true`（会触发 Linear GraphQL 复杂度 15880 > 10000 报错）。
- **Comment on issue (`save_comment`)**:
  - 参数为 `{"issueId": "STARSRC-...", "body": "..."}`。
- **Apply triage labels**:
  - 使用 [docs/agents/triage-labels.md](triage-labels.md) 中记录的标签名或 UUID，通过 `save_issue` 的 `labels` 数组传入。
