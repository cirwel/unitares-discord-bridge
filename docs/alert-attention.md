# Reading the existing alert channels

Start with **#alerts** for urgent triage. You do not need to treat every resident
channel as another inbox. These are different views of evidence, not independent
assignments. No new channel or resident is needed.

| Existing channel | What it is for | What to do |
|---|---|---|
| #alerts | Pauses, stuck/silent-critical agents, critical/high findings, human-review requests | Check the named subject, time and evidence; identify one owner for the underlying condition. |
| #signals | Diagnostic transitions such as drift, risk and adjustment | Consult when investigating an alert; not another urgent task queue. |
| #sentinel, #doctor, #residents | Resident findings and supporting detail | Consult the relevant producer. High/critical findings are mirrored to #alerts. |
| #activity, #governance-hud | Routine events and current reported state | Context, not proof every job completed successfully. |
| #lumen-sensors | Lumen observations and offline/recovery transitions | This is still the existing Lumen outage destination; do not assume all Lumen outages appear in #alerts. |
| #lumen-iterations | Read-only proposal/review/canary attention | A post or reaction is not a signed review or permission to deploy. |

## One condition, not one task per message

An alert and its resident detail can describe the same condition. The bridge now
coalesces exact REST/WebSocket finding copies **per destination**, after a
successful Discord send. Both transports remain available as fallbacks. A failed
send does not suppress the other transport. Changed timestamps, severities,
messages, subjects or destinations are not discarded. The bounded cache is
process-local: it is not exactly-once delivery across restarts or uncertain
network outcomes, nor a cross-incident mute. Suppression receipts retain the
delivered message ID and reason `duplicate_finding_delivery`.

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
