# Reading the existing alert channels

Start with **#alerts** for urgent triage. You do not need to treat every resident
channel as another inbox. These are different views of evidence, not independent
assignments. No new channel or resident is needed.

| Existing channel | What it is for | What to do |
|---|---|---|
| #alerts | Pauses, stuck/silent-critical agents, critical findings, residents' ≥70% risk crossings, human-review requests | Check the named subject, time and evidence; identify one owner for the underlying condition. |
| #signals | Diagnostic transitions such as drift, risk and adjustment, for resident agents | Consult when investigating an alert; not another urgent task queue. |
| #sentinel, #doctor, #residents | Resident findings, each posted once | Consult the relevant producer. A critical finding goes to #alerts instead of here, not in addition. |
| #activity, #governance-hud | Routine events (new *resident* agents, archives, knowledge writes) and current reported state | Context, not proof every job completed successfully. |
| #lumen-sensors | Lumen observations, offline/recovery transitions, and Lumen's own drift-oscillation / trajectory-adjustment chatter | This is still the existing Lumen outage destination; do not assume all Lumen outages appear in #alerts. |
| #lumen-iterations | Read-only proposal/review/canary attention | A post or reaction is not a signed review or permission to deploy. |

## What is deliberately not posted

These are receipted to governance as `bridge.suppressed` (with a reason) and
never posted, so the volume is still auditable:

- `agent_new` and `risk_threshold` for agents governance does not mark as
  resident (reason `non_resident_agent`). The bridge reads the subject agent's
  governance tags with `agent(action="get")`: an agent tagged `persistent`,
  `protected` or `embodied` is a resident; a short-lived session (tagged
  `ephemeral`) is not. If the tags cannot be read, the event posts as before.
- Resolution findings whose action is `dismissed` (Watcher's "[dismissed] ..."
  closures; reason `dismissed_resolution`). Confirmed resolutions still post,
  to the resident channel only.

Settings (all optional): `BRIDGE_RESIDENT_ONLY_EVENT_TYPES`,
`BRIDGE_RESIDENT_AGENT_TAGS`, `BRIDGE_EMBODIED_STATE_EVENT_TYPES`,
`BRIDGE_EMBODIED_AGENT_TAGS`, `BRIDGE_EMBODIED_STATE_CHANNEL`,
`BRIDGE_FINDING_ALERT_SEVERITIES` (default `critical`) and
`BRIDGE_SUPPRESS_DISMISSED_RESOLUTIONS`; see `src/bridge/config.py`.

## One condition, not one task per message

An alert and its resident detail can describe the same condition. The bridge now
coalesces exact REST/WebSocket finding copies **per destination**, after a
successful Discord send. Both transports remain available as fallbacks. A failed
send does not suppress the other transport. Changed timestamps, severities,
messages, subjects or destinations are not discarded. Confirmed deliveries are
also kept in the bridge's SQLite cache for seven days, so a restart does not
re-post what the other transport already delivered. It is still not
exactly-once delivery across uncertain network outcomes, nor a cross-incident
mute. Suppression receipts retain the delivered message ID and reason
`duplicate_finding_delivery`.

Each finding has exactly one destination: #alerts when its severity is in
`BRIDGE_FINDING_ALERT_SEVERITIES` (default `critical`), otherwise its resident
channel. Findings are no longer mirrored to a second channel.

WebSocket findings use the same full message/evidence renderer and destination
rules as polled findings. This avoids the previous pair of a detail-free lowercase
title followed by a second, complete resident message.

## What an alert does—and does not—mean

- **Risk threshold:** governance state crossed a threshold. It is not an
  externally verified task failure or a confirmed service outage.
- **Silent critical:** check whether this subject should currently be active,
  its configured cadence, and its last completed work. Do not automatically
  restart it or weaken the silence threshold.
- **Resident finding:** read severity and supporting evidence; a report alone
  does not prove independent corroboration or that repair is authorized.
- **Human review requested:** follow the existing review route. An emoji cannot
  approve a proposal, resume an agent, or authorize a production change.

Use ✅ to mean **seen**, not **fixed**. Resolution needs evidence that the named
failure was corrected, with the responsible owner and repair/deployment receipt.
The bridge records acknowledgements when governance is reachable; historical
misclassified receipts are not rewritten. Critical routing now produces matching
high/critical receipt severity instead of silently defaulting missing severity
to `info`. Bounded receipt queries must be read with `limit_reached`; a truncated
sample or zero count does not establish an empty historical backlog.

## Notification and independence gaps

A Discord post is not proof of a phone notification. The existing
`LUMEN_OFFLINE_MENTION` applies only to Lumen offline/recovery transitions;
general alert posts do not acquire mentions from that setting. Verify the actual
recipient and notification settings with one clearly labelled, authorized test,
then check the human reaction and matching acknowledgement receipt. Never have
an agent acknowledge on the operator's behalf or fabricate a production outage.

Residents detect and explain; the bridge delivers. Both may fail with the same
host or network. The local bridge-liveness watchdog can restart a hung bridge,
but does not replace an external missed-heartbeat check. Repair/deploy authority,
governance rules, credentials and operator stops remain unchanged. No autonomous
repair loop is introduced by these delivery fixes.

## Making the navigation guide easy to find

Pin the existing navigation message in `#alerts`; do not post repeated copies.
Discord requires the separate **Pin Messages** permission (`PIN_MESSAGES`),
not Administrator or a broader message-deletion grant. If the bridge lacks it,
the server owner can either pin the guide directly or allow **Pin Messages**
for the bridge member in `#alerts` only. Leave all other permissions unchanged.
Read back the message's `pinned` state before reporting success.

The bot cannot normally grant permissions it does not already hold in the guild
or parent channel. Do not work around that boundary by self-granting an override
for Manage Permissions, obtaining a human token, or widening server roles.
See Discord's [pin-message API](https://docs.discord.com/developers/resources/message#pin-message)
and [channel permission rules](https://docs.discord.com/developers/resources/channel#edit-channel-permissions).
Pinning is navigation, not verification that alerts reach a phone.
