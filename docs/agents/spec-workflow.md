# Spec-Driven Development

All non-trivial work in this repo follows a spec-first workflow. No implementation without a spec; no spec without agreed test seams.

## Workflow

1. **Spec** (`/to-spec`) — synthesize the current conversation into a spec and publish it as a GitHub issue with the `ready-for-agent` label. The spec defines the problem, solution, user stories, implementation decisions, testing decisions, and out-of-scope boundaries.

2. **Agree seams** — before any code, identify the public interfaces (seams) where tests will live. Confirm them with the user. No test is written at an unconfirmed seam.

3. **Implement** (`/implement`) — build from the spec using TDD at the agreed seams.

4. **TDD loop** (`/tdd`) — red-green at each seam. One test, one minimal implementation, repeat. No speculative code.

5. **Review** (`/code-review`) — review the finished work against the spec.

## Rules

- **If it's not in the spec, don't build it.** Scope creep is caught by checking every change against the spec's user stories and out-of-scope section.
- **Seams before code.** Test boundaries are a design decision, not an afterthought. Prefer existing seams over new ones; prefer the highest seam possible.
- **Specs are living documents.** If implementation reveals a gap, update the spec issue first, then proceed. The spec is the source of truth, not the conversation.
- **Vertical slices, not horizontal layers.** Each TDD cycle delivers one user-visible behavior end-to-end, not a layer across many behaviors.
- **Domain vocabulary.** Specs use terms from `CONTEXT.md`. If a term isn't in the glossary, that's a signal to run `/domain-modeling` before proceeding.
