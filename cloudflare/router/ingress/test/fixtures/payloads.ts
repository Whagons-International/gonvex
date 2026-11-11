// Provider-shaped webhook bodies, kept byte for byte as the providers format
// them (Telnyx: compact JSON with `data`/`meta`; GitHub: pretty-printed JSON
// with a trailing newline). Telnyx bodies are signed in the tests with a
// fixed test key; no real Telnyx signature can be recorded without a Telnyx
// account key. GitHub's own documented vector is recorded verbatim below.

/** Telnyx Call Control `call.initiated` (Telnyx API v2 webhook envelope). */
export const TELNYX_CALL_INITIATED =
  '{"data":{"event_type":"call.initiated","id":"0ccc7b54-4df3-4bca-a65a-3da1ecc777f0","occurred_at":"2026-11-11T18:10:23.127000Z","payload":{"call_control_id":"v3:MdI91X4lWFEs7IgbBEOT9M4AigoY08M0WWZFISt1Yw2axZ_IiE4pqg","call_leg_id":"428c31b6-7af4-4bcb-b7f5-5013ef9657c1","call_session_id":"428c31b6-abf3-4bcb-b7f5-5013ef9657c1","client_state":"aGF2ZSBhIG5pY2UgZGF5ID1d","connection_id":"7267xxxxxxxxxxxxxx","direction":"incoming","from":"+35319605860","start_time":"2026-11-11T18:10:23.127000Z","state":"parked","to":"+35319605861"},"record_type":"event"},"meta":{"attempt":1,"delivered_to":"https://ingress.test/webhooks/telnyx"}}';

/** `call.hangup`, with a non-ASCII caller name and escaped characters left as sent. */
export const TELNYX_CALL_HANGUP =
  '{"data":{"event_type":"call.hangup","id":"9f1c2c55-1d3e-4b8a-9f3c-6a1f2b9e7d10","occurred_at":"2026-11-11T18:12:01.009000Z","payload":{"call_control_id":"v3:MdI91X4lWFEs7IgbBEOT9M4AigoY08M0WWZFISt1Yw2axZ_IiE4pqg","caller_id_name":"José Müller 📞","hangup_cause":"normal_clearing","hangup_source":"caller","sip_hangup_cause":"200","note":"line\\nbreak \\u00e9 \\"quoted\\" \\/slash"},"record_type":"event"},"meta":{"attempt":2,"delivered_to":"https://ingress.test/webhooks/telnyx"}}';

/** GitHub's documented signature test vector (docs: "Validating webhook deliveries"). */
export const GITHUB_DOC_VECTOR = {
  secret: "It's a Secret to Everybody",
  body: "Hello, World!",
  signature: "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
};

/** A `ping` delivery as GitHub formats it (indented JSON, trailing newline). */
export const GITHUB_PING = `{
  "zen": "Design for failure.",
  "hook_id": 512345678,
  "hook": {
    "type": "Repository",
    "id": 512345678,
    "name": "web",
    "active": true,
    "events": [
      "pull_request",
      "pull_request_review",
      "check_run",
      "check_suite",
      "status"
    ],
    "config": {
      "content_type": "json",
      "insecure_ssl": "0",
      "url": "https://ingress.test/webhooks/github?tenant=alpha"
    }
  },
  "repository": {
    "id": 1296269,
    "full_name": "octo-org/other-repo"
  }
}
`;

/** A `pull_request` delivery for a repository the connection does not allow. */
export const GITHUB_PULL_REQUEST = `{
  "action": "opened",
  "number": 7,
  "pull_request": {
    "number": 7,
    "title": "Fix the lobby door (WH-12)",
    "user": { "login": "octocat" },
    "head": { "sha": "6dcb09b5b57875f334f61aebed695e2e4193db5e" }
  },
  "repository": { "id": 1296269, "full_name": "octo-org/other-repo" }
}
`;
