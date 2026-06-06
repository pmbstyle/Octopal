# Telegram Bot Setup

This guide covers the practical Telegram setup for an Octopal agent: creating a bot,
getting the token, finding chat IDs, allowing private and group chats, and enabling
multi-agent group chat behavior.

## 1. Create a Bot and Get the Token

1. Open Telegram and start a chat with `@BotFather`.
2. Run `/newbot`.
3. Pick a display name and username for the bot.
4. Copy the token returned by BotFather. It looks like:

   ```text
   <telegram-bot-token>
   ```

5. Put the token in `config.json`:

   ```json
   {
     "user_channel": "telegram",
     "telegram": {
       "bot_token": "<telegram-bot-token>",
       "allowed_chat_ids": [],
       "parse_mode": "MarkdownV2"
     }
   }
   ```

Keep the token secret. Anyone with the token can control the bot.

## 2. Get Your Private Chat ID

The private chat ID is the user chat where you talk directly to the bot.

Simple path:

1. Open Telegram and message `@userinfobot` or `@RawDataBot`.
2. Copy your numeric user ID.
3. Add it to `telegram.allowed_chat_ids`.

Bot API path:

1. Start a private chat with your bot and send any message.
2. Stop Octopal if it is already polling with this token. Otherwise Octopal may
   consume the update before your manual `getUpdates` call sees it.
3. Run:

   ```bash
   curl "https://api.telegram.org/bot<token>/getUpdates"
   ```

4. Find `message.chat.id` in the response.

Example private config:

```json
{
  "telegram": {
    "bot_token": "<telegram-bot-token>",
    "allowed_chat_ids": ["<private-chat-id>"],
    "parse_mode": "MarkdownV2"
  }
}
```

If `allowed_chat_ids` is empty, Telegram access is effectively open. Set it before
running a real deployment.

## 3. Start and Check Telegram

Start Octopal:

```bash
uv run octopal start
```

Check status and logs:

```bash
uv run octopal status
uv run octopal logs --follow
```

If the bot starts but does not reply:

- Verify `telegram.bot_token`.
- Verify the private or group chat ID is listed in `telegram.allowed_chat_ids`.
- Check that `user_channel` is `telegram`.
- Tail logs with `uv run octopal logs --follow`.

## 4. Group Chat Setup

Octopal can run in Telegram groups, but the group chat ID must be allowed.

1. Add the bot to the group.
2. Send a message in the group.
3. Get the group chat ID with one of these methods:
   - Add `@RawDataBot` or a similar diagnostic bot to the group and read the group ID.
   - Use Bot API `getUpdates` while Octopal is stopped, after the bot receives a
     group message:

     ```bash
     curl "https://api.telegram.org/bot<token>/getUpdates"
     ```

     Look for `message.chat.id`.

Group chat IDs are usually negative numbers, for example:

```json
{
  "telegram": {
    "bot_token": "<telegram-bot-token>",
    "allowed_chat_ids": ["<private-chat-id>", "<group-chat-id>"],
    "parse_mode": "MarkdownV2"
  }
}
```

If the bot does not receive ordinary group messages, open `@BotFather`, choose the
bot, and disable Group Privacy Mode. With the classic BotFather command flow:

```text
/setprivacy
```

Choose the bot, then choose `Disable`.

After changing privacy mode, remove the bot from the group and add it back if
Telegram does not apply the new setting immediately.

## 5. Group Addressing

Group chats use an addressing gate so the agent does not answer every ambient
message in the group.

The agent responds when the message is addressed to:

- The agent's configured name.
- Any configured agent alias.
- A configured collective alias, such as `Octopals` or `agents`.
- A reply to one of the agent's messages.

Messages that are visible but not addressed to the agent are recorded as passive
group context when Telegram delivers them, but they do not trigger a reply.

Example:

```json
{
  "group_addressing": {
    "enabled": true,
    "agent_name": "<agent-name>",
    "agent_aliases": ["<agent-name>", "<agent-bot-username>"],
    "collective_aliases": ["Octopals", "agents", "AI agents"]
  }
}
```

Configure aliases explicitly. Octopal does not add hidden default aliases, so include
the names and collective labels you want the agent to recognize.

## 6. Multi-Agent Group Chats

For a group containing several Octopal agents:

1. Add each bot to the group.
2. Add the same group chat ID to each agent's `telegram.allowed_chat_ids`.
3. Configure each agent's `group_addressing.agent_name` and aliases.
4. Configure shared `collective_aliases` on each agent if you want one message to
   address all agents.

Example behavior:

- `Agent A, summarize this` -> only Agent A should respond.
- `Octopals, pull latest and restart` -> all agents with `Octopals` configured as a
  collective alias should respond.
- `Agent A and Agent B, what do you think?` -> agents named or aliased as Agent A and
  Agent B should respond; other agents should observe but stay silent.

## 7. Bot-to-Bot Communication

Telegram normally does not deliver messages from one bot to another bot. This matters
for multi-agent groups: without Bot-to-Bot Communication Mode, an agent may see human
messages in the group but miss replies written by the other agents.

Enable this for every agent bot:

1. Open `@BotFather`.
2. Open BotFather's Mini App.
3. Select the bot.
4. Enable `Bot-to-Bot Communication Mode`.
5. Repeat for every agent bot.

For receiving all bot-authored messages in a group without explicit mentions or
replies, the receiving bot also needs one of these:

- Admin rights in the group.
- Group Privacy Mode disabled.

Recommended multi-agent setup:

- Enable Bot-to-Bot Communication Mode for every agent bot.
- Make every agent bot an admin in the group, or disable Group Privacy Mode for every
  agent bot.
- Keep Octopal's group addressing enabled to prevent reply loops.

Octopal records non-addressed group messages, including messages from other bots when
Telegram delivers them, as passive group observations. These observations are shared
conversation context, not direct user turns, so agents can understand the group chat
without replying to every message.

In the current Telegram channel implementation, bot-authored group messages are
observation-only. Octopal stores them as context and does not answer them directly,
which helps prevent bot-to-bot reply loops.

Telegram reference:

- https://core.telegram.org/bots/features
- https://core.telegram.org/bots/faq

## 8. Scheduled Messages to Groups

Scheduled tasks can optionally post to a Telegram group. Add the group chat ID to
`telegram.allowed_chat_ids`, then ask the agent to send that scheduled update to the
group.

Deployments that do not use Telegram groups do not need this setting.

## 9. Supported Telegram Features

### Commands

The bot supports:

- `/help` - Show available bot commands.
- `/status` - Show runtime status, PID, heartbeat, and worker summary.
- `/workers` - Show discovered worker templates and recent workers.
- `/memory [limit]` - Show a memory snapshot summary.
- `/version` - Show the current bot version.

Group commands are also subject to group addressing. A plain group command that is not
addressed to the agent is ignored.

### Silent Memory Mode

Start a message with `! ` or `> ` to save a note without triggering a full
conversation turn.

Example:

```text
! The staging server uses port 8080.
```

The bot reacts with a writing-hand emoji and does not generate a text reply.

### Images and Files

Telegram photos and supported attachments are saved and passed into the Octo prompt.
You can ask questions about the image or refer back to saved files in later turns.

### Reactions

The Octo can react to messages with emojis in addition to, or instead of, a text
reply. Telegram reactions are also used as immediate feedback while a turn is being
processed.

### Progress Updates

Octopal tracks worker and runtime progress events such as:

- `queued`
- `running`
- `completed`
- `failed`
- `duplicate`
- `worker_started`

User-facing progress is primarily delivered through the normal reply flow. Telegram
logs progress events for observability.
