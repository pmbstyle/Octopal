# Runtime Plans

Runtime plans are Octopal's durable execution contract for user tasks that require action.
They are intentionally different from saved workflows:

- A runtime plan is created for one concrete user request.
- A saved workflow is a reusable recipe that may create runtime plans later.

The first goal is reliability: when Octo says she will do work, the runtime should have a
state object that can be continued, inspected, or marked blocked instead of relying on chat
context alone.

## Core Model

### Plan run

A `plan_run` represents one user-facing goal.

Important fields:

- `id`
- `goal`
- `status`
- `chat_id`
- `correlation_id`
- `current_step_id`
- `plan_json`
- `metadata_json`

Primary statuses:

- `planned`: the plan exists but no step is running yet.
- `running`: Octo or the runtime is actively working on a step.
- `needs_next_step`: a step completed and Octo should continue from the next pending step.
- `awaiting_worker`: a worker step is running and the plan should resume when it returns.
- `awaiting_approval`: execution is stopped on a hard approval.
- `awaiting_user`: execution is stopped waiting for missing user input.
- `blocked`: Octo cannot safely continue without changed context.
- `completed`, `failed`, `cancelled`: terminal states.

### Plan step

A `plan_step` is one bounded action inside a run.

Important fields:

- `step_id`
- `seq`
- `kind`
- `title`
- `task`
- `executor`
- `worker_run_id`
- `input_json`
- `output_json`
- `status`

Initial step kinds:

- `octo`: reasoning, synthesis, or final response by Octo.
- `tool`: deterministic Octo tool call.
- `worker`: delegated worker task.
- `approval`: hard stop before a risky or externally visible action.
- `input`: hard stop for missing user input.
- `final`: final user response.

### Plan event

`plan_events` are append-only breadcrumbs for dashboard/debugging:

- `plan.created`
- `step.started`
- `step.awaiting_worker`
- `step.completed`
- `step.failed`
- `plan.completed`

## Execution Shape

The plan executor should be small and deterministic:

1. Find the active plan for the chat/correlation.
2. Load `current_step_id` and previous step outputs.
3. Ask Octo to execute or decide only the next bounded step.
4. Persist the step result before doing anything else.
5. Move to `needs_next_step`, `awaiting_worker`, `awaiting_approval`, `awaiting_user`, or a terminal state.

This lets incoming events interrupt without destroying focus. A worker result can update its
own step, then the runtime can continue the same plan from the next step.

## Interrupt Semantics

New user messages should not automatically erase active plans.

Default behavior:

- If the new message is clearly about the active plan, treat it as steering or extra input.
- If it is unrelated and the active plan is waiting on a worker/approval/input, handle the new
  message normally and keep the plan paused.
- If it is unrelated while the plan is actively running, either queue it as a separate turn or ask
  Octo whether to interrupt/replan.

The invariant is simple: unrelated work should not silently cancel or overwrite a prior plan.

## Integration Path

1. Durable state foundation: `plan_runs`, `plan_steps`, `plan_events`, and `PlanRunService`.
2. Planning tool: let Octo create an ad-hoc plan from the current task.
3. Continuation prompt: inject active plan state into `route_or_reply` when a plan is active.
4. Worker binding: when a worker starts for a plan step, store `worker_run_id` on that step.
5. Worker result resume: when the worker result arrives, complete the matching step and route a
   plan continuation instead of an unstructured follow-up.
6. Watchdog: heartbeat/scheduler finds stale active plans and routes a continuation or marks a
   clear blocked state.
7. Saved workflows: reusable YAML recipes can create runtime plans after the runtime contract is stable.

## Design Constraints

- Plans are not arbitrary scripts.
- Shell execution is not a plan primitive; use normal tools or isolated workers.
- Hard approvals are runtime states, not prompt suggestions.
- Worker isolation and `allowed_paths` rules stay unchanged.
- Plan state should be compact enough to inject into prompts without reloading full history.
