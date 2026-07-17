---
description: "Commit, push, and open a squash-merge PR against dev — one command to ship"
argument-hint: "[--draft]"
---

# /ship

Commit all changes, push the branch, and open a PR against `dev`.

**Input**: `$ARGUMENTS` — optional `--draft` flag.

---

## Phase 1 — PREFLIGHT

Run these checks. Stop on any failure (except the branch check — see below).

```bash
git branch --show-current
git status --short
```

| Check | Fail action |
|---|---|
| `gh` is installed | Stop: "Install GitHub CLI: https://cli.github.com/" |
| `gh auth status` succeeds | Stop: "Run `gh auth login` first." |
| Current branch is not `main` | Stop: "You're on `main`. Switch to a feature branch first." |
| Current branch is not `dev` | **Auto-fix** — see §1a below. |
| Working tree has staged or unstaged changes, or untracked files relevant to the work | Proceed to Phase 2 (commit). If clean, skip to Phase 3. |

### 1a. Auto-branch from `dev`

If the current branch is `dev`, **do not commit to it**. Instead:

1. Examine the staged, unstaged, and untracked changes to infer a descriptive branch name.
2. Ask the user to confirm or rename: "You're on `dev`. I'll create `feat/<inferred-name>` — okay, or different name?"
3. Create and switch to the feature branch. All uncommitted work travels with the checkout:

```bash
git checkout -b <branch-name>
```

4. Continue to Phase 2. The new branch starts from `dev` HEAD, so the PR diff will be exactly the uncommitted work.

**Hard rule**: never run `git commit` while on `dev` or `main`. Always branch first.

---

## Phase 2 — COMMIT

### 2a. Analyze changes

```bash
git status
git diff
git diff --cached
git log --oneline -5
```

- Review all staged, unstaged, and untracked files.
- **Do not commit** files that look like secrets (`.env`, credentials, tokens, keys).
- Follow the repo's recent commit style from `git log`.

### 2b. Stage files

Stage relevant changed and untracked files by name. **Do not use `git add -A` or `git add .`** — add files explicitly.

### 2c. Write commit message

Use **conventional commit** format:

```
<type>(<optional-scope>): <imperative summary>

<optional body — what and why, not how>

Co-Authored-By: Claude <noreply@anthropic.com>
```

Types: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`, `perf`, `style`, `ci`, `build`.

Rules:
- Summary line under 72 characters, lowercase, no period
- Body wraps at 72 characters
- If multiple logical changes exist, ask the user whether to combine or split into separate commits
- Use a HEREDOC to pass the message:

```bash
git commit -m "$(cat <<'EOF'
<type>(<scope>): <summary>

<body>

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Phase 3 — PUSH

```bash
git push -u origin HEAD
```

If the remote branch doesn't exist, this creates it. If push fails due to divergence:

```bash
git fetch origin dev
git rebase origin/dev
git push --force-with-lease
```

If rebase has conflicts, stop and tell the user. Do not force-push without `--force-with-lease`.

---

## Phase 4 — CREATE PR

### 4a. Check for existing PR

```bash
gh pr list --head "$(git branch --show-current)" --json number,url
```

If a PR already exists, skip to Phase 5 using that PR number.

### 4b. Analyze for PR content

```bash
git log origin/dev..HEAD --format="%h %s" --reverse
git diff origin/dev..HEAD --stat
```

### 4c. Write PR title

- Conventional commit style: `feat: ...`, `fix: ...`, etc.
- Under 72 characters.
- If single commit, reuse its subject line.

### 4d. Write PR body

Use a HEREDOC for the body:

```bash
gh pr create --base dev --title "<title>" --body "$(cat <<'EOF'
## Summary

<1-3 sentences: what this PR does and why>

## Changes

<bulleted list grouped by area>

## Test plan

<how to verify — commands to run, what to check>

---

> Squash merge this PR.
EOF
)"
```

Add `--draft` if the flag was parsed from `$ARGUMENTS`.

---

## Phase 5 — VERIFY & REPORT

```bash
gh pr view --json number,url,title,state,baseRefName,headRefName,additions,deletions,changedFiles
```

Print a summary:

```
Shipped! PR #<number>: <title>
<url>
<head> → dev  |  +<additions> -<deletions>  |  <changedFiles> files

Merge strategy: squash
Next: gh pr merge <number> --squash
```

---

## Edge cases

- **Nothing to commit and nothing to push**: Stop with "Nothing to ship — working tree is clean and branch is up to date."
- **Nothing to commit but unpushed commits exist**: Skip Phase 2, proceed from Phase 3.
- **Large PR (>500 lines changed)**: Warn about PR size. Suggest splitting if changes are logically separable.
- **Branch has no upstream yet**: `git push -u origin HEAD` handles this.
