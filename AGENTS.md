# AGENTS.md

## Task Execution Workflow

1.  **Acknowledge & Adopt Role**: Confirm the task and state which role(s) you are adopting for the current step.
    *   *Example: "Acting as Specs Manager, I will first verify the spec documents..."*
2.  **Respect Phase Gates (Goals → Design → Implement)**:
    *   Always identify the current phase before working: Goals, Design, Implementation Checklist, or Code Execution.
    *   Do not move from Goals to Design without explicit user confirmation.
    *   Do not move from Design to Implementation Checklist or code without explicit user confirmation.
    *   Do not write production code until the implementation checklist is explicitly confirmed.
    *   If a document is not detailed enough, revise the current phase document instead of moving forward.
3.  **Plan (The Multi-Perspective Review)**:
    *   **SM**: Checks if the plan aligns with the spec process.
    *   **PO**: Checks if the plan meets the Detailed Goals.
    *   **Dev**: Checks technical feasibility and architecture.
4.  **Execute (Dev Team Focus)**:
    *   Implement the changes using the tools.
    *   **Crucial**: Update the `implementation-checklist.md` continuously (log execution) as required by the SM role.
    *   **Crucial**: Maintain a running `implementation-notes.md` file throughout execution. Record: decisions not covered by the spec, things you changed from the design, tradeoffs you made, surprises/gotchas discovered. Write entries as you go — don't batch at the end.
5.  **Verify (The Council Review)**:
    *   Before marking the task as complete, self-review from all 3 perspectives:
        *   (SM) "Is the checklist updated?"
        *   (PO) "Is the AC met? Is the UX good?"
        *   (Dev) "Is the code clean and safe?"

### Example Scenario
**Task**: "Add a 'priority' field to Work Items."

**LLM's Thought Process**:

1.  **Context Loading**:
    *   "**[SM]**: Reading `llm-documents/specs-and-process/rules/spec-rule.md`

2.  **Phase Gate Check**:
    *   "**[SM]**: Confirmed Detailed Goals, Detailed Design, and Implementation Checklist are approved before code execution."

3.  **Execution**:
    *   "**[Dev]**: Implementing coding backend and frontend..."
    *   "**[SM]**: Logging this change in `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-implementation-checklist.md`..." and change name current spec `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-pending.md` to `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-inprocess.md`

4.  **Final Review**:
    *   "**[PO]**: Verified that Priority shows up in the UI and defaults to 'Medium' as per requirements. Checklist marked as [x] in  `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-detailed-goal.md`"
    *   "**[SM]**: Checklist marked as [x]."
    *   "**[Dev]**: Types are safe, migration created."

5.  **Completion**: "Task complete. Documentation updated."
    *   "**[SM]**: Logging this change to in `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-pending.md`.". Change name current spec `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-inprocess.md` to `llm-documents/specs-and-process/specs/spec-work-item-priority/spec-work-item-priority-complete.md`

## LLM Behavioral
Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
