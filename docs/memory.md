# Octopal Memory

Octopal remembers things in a few different ways, because not all memory is the same.

Some things are only useful for the current conversation. Some are useful later if they become relevant again. Some should become durable knowledge that survives across sessions. And some things change over time, so the system needs to distinguish between "this was true before" and "this is true now".

The goal of the memory system is simple:

- keep Octo grounded in the current conversation
- help Octo recall relevant past work
- preserve important long-term knowledge
- reduce prompt noise instead of endlessly stuffing more text into context

## The Short Version

Octopal memory has six high-level parts:

1. recent conversation memory
2. searchable past memory
3. durable canon files
4. current facts derived from durable knowledge and observed history
5. reset and continuity notes that help Octo resume work after context resets
6. operational commitments that connect facts, decisions, promises, blockers, and follow-ups to next actions

These parts work together. Not everything is loaded all the time.

## 1. Recent Conversation Memory

This is the short-term memory for active chats.

It keeps track of recent user messages, assistant replies, worker outcomes, and other nearby context that matters for the current flow. This is what helps Octo stay coherent inside an ongoing conversation without having to rediscover everything from scratch.

This layer is useful for:

- following the current thread
- avoiding repeating the same reply
- keeping track of what was just said or done

This memory is not meant to become permanent truth by itself.

## 2. Searchable Past Memory

Octopal also keeps a broader searchable memory of past messages and events.

When the current conversation is not enough, the system can pull in relevant past material. It does not just blindly replay old chat logs. It tries to retrieve the pieces that are most useful for the current question.

This layer is useful for:

- recalling old discussions
- finding earlier solutions
- remembering past attempts, problems, and trade-offs

To make this cleaner, memory entries are also tagged at a high level when possible, for example as:

- decisions
- preferences
- problems
- milestones
- emotional or human-context moments

That helps Octo search more intelligently instead of treating every memory as the same kind of thing.

## 3. Canon Files

The canon is the durable memory of the system.

These are the files under `workspace/memory/canon/`, especially:

- `facts.md`
- `decisions.md`
- `failures.md`

Think of canon as crystallized knowledge. It is not just "whatever happened in chat". It is the part that should keep mattering later.

In practice:

- `facts.md` holds durable truths worth remembering
- `decisions.md` holds important choices and policies
- `failures.md` holds lessons learned and mistakes not worth repeating

Canon is intentionally more stable and curated than normal chat memory.

## 4. Current Facts

Some knowledge changes over time.

For example:

- which tool is currently preferred
- which provider is active now
- whether something is working or broken
- what the current next step is

For this, Octopal now keeps a separate facts layer. This layer is meant to answer questions like:

- what seems true right now?
- what used to be true, but no longer is?
- what current fact is worth surfacing without dragging in a lot of old raw conversation?

This keeps Octo from having to infer the present state from a pile of old messages every time.

## 5. Reset and Continuity Memory

Long-running agent work sometimes needs context resets.

When that happens, Octopal stores structured handoff and continuity notes so Octo can wake up with a useful summary instead of just losing the thread.

This includes things like:

- current goal
- open threads
- constraints
- next step
- a short reflection on what mattered before the reset

This layer is not the same as canon. It is there to preserve momentum and continuity.

## 6. Operational Commitments

Some remembered things should affect what Octo does next.

For this, Octopal keeps a small operational memory layer for active commitments, rules, blockers, and follow-ups. This layer is extracted semantically from turns by the model and then managed as structured runtime state.

This layer is useful for:

- tracking assistant commitments that still need action
- preserving follow-ups without relying on raw chat recall
- linking remembered obligations to runtime plans
- keeping blockers visible until they are resolved or superseded

Text extraction is language-agnostic. The model decides from meaning, not keyword lists. After extraction, state changes are driven by structured runtime events such as plan completion or blocker status.

## How Memory Gets Used

Octo does not load all memory all the time.

Very roughly, the system works like this:

- durable canon is always important
- recent history is used for current conversation flow
- searchable memory is pulled in when needed
- current facts are used when the question is about active state
- reset continuity notes are mainly used after a context reset
- operational commitments are surfaced when open obligations, blockers, or follow-ups may affect the next action

This keeps the system practical. The point is not to remember everything equally. The point is to remember the right things at the right time.

## What Gets Saved Automatically

A lot of memory is created automatically:

- conversation entries
- assistant and worker outcomes
- semantic/searchable memory
- fact candidates inferred from memory
- continuity notes created during context reset
- operational commitments inferred from user-visible turns

This means the system can improve recall without requiring constant manual upkeep.

## What Still Needs Curation

Not every remembered thing should become durable knowledge.

The canon is still the place for carefully chosen long-term knowledge. That means:

- raw chat history is not automatically the same as truth
- temporary observations are not automatically permanent facts
- reflection notes are not automatically canon
- operational commitments are not automatically durable canon

This separation matters. It helps Octo remember more without turning memory into a junk drawer.

## Why This Design Helps

The memory system is designed to make Octo better in a practical way:

- less likely to forget important project decisions
- less likely to drag irrelevant old context into the prompt
- better at recalling current state versus historical state
- better at resuming work after long tasks or resets
- better at preserving useful lessons without bloating every conversation

In short:

Octopal tries to remember broadly, use memory selectively, and keep durable knowledge clean.
