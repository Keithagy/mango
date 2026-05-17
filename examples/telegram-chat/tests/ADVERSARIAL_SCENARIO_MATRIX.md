| id | category | scenario | expected behavior | status |
|---|---|---|---|---|
| AX-01 | Photo classification | Caption-only photo hint like `Lunch 12.50` on a non-receipt image | Do not auto-save; require clarification or fall through without mutation | `bdd-now` |
| AX-02 | Intent arbitration | Mixed command like `show and delete Acme Lunch` | Ask for one action at a time; do not mutate | `bdd-now` |
| AX-03 | Lexical overlap | Merchant name collides with command word, e.g. `Delete Cafe` or `Set Lunch` | Read/update the matching expense instead of misclassifying by merchant text | `bdd-now` |
| AX-04 | Negation | `don't delete Acme Lunch` or `do not change Acme..., just show it` | Never mutate on a negated destructive request | `bdd-now` |
| AX-05 | Clarification answer shape | Question-form answer like `Is 12.50 okay?` while amount is missing | Keep clarification open; do not save yet | `bdd-now` |
| AX-06 | Clarification cancellation | `not an expense`, `cancel`, `ignore it` while a draft clarification is pending | Clear the clarification without saving | `bdd-now` |
| AX-07 | Clarification persistence | Partial answer narrows the missing fields, unrelated chat falls through, later answer still saves | Preserve narrowed clarification state across unrelated turns | `covered` |
| AX-08 | Clarification vs explicit read | `show my expenses` while a receipt clarification is pending | Handle the explicit read without consuming the clarification | `covered` |
| AX-09 | Selection clarification persistence | Ambiguous update/delete, then interleaved `show my expenses`, then explicit selection | Preserve the pending `ChooseExpense` clarification across the read | `backlog` |
| AX-10 | Unsupported selection answer | `first one` / `the 12.50 one` while choose-one clarification is pending | Keep the clarification active and ask for an exact id or merchant | `backlog` |
| AX-11 | Clarification overwrite | Pending receipt clarification, then a new ambiguous delete/update clarification arrives | Do not silently overwrite the earlier clarification state | `backlog` |
| AX-12 | Duplicate replay before save | Same vague receipt photo resent before the first clarification is resolved | Converge to one pending workflow and one eventual save | `backlog` |
| AX-13 | Duplicate replay after save | Same saved receipt resent with a different caption | Reply duplicate; do not mutate existing markdown | `backlog` |
| AX-14 | Session isolation | Same chat but different thread uses a shared dispatcher/state root | Keep separate automation state per thread | `backlog` |
| AX-15 | Whitelist isolation | Intruder in same chat/thread tries to answer another user’s pending clarification | Reject intruder; leave trusted clarification intact | `bdd-now` |
| AX-16 | Username normalization | Trusted username variants like ` @Trusted_Customer ` | Normalize and accept as trusted | `backlog` |
| AX-17 | Persistence integrity | Caption containing TOML/frontmatter-looking text | Persist parseable markdown and preserve later reads | `backlog` |
| AX-18 | Ledger corruption isolation | One malformed markdown file beside valid expenses | Avoid destructive recovery; preserve readable entries | `backlog` |
| AX-19 | Mutation failure persistence | Store write fails during create/update/delete | Preserve retryable clarification/selection state; no partial writes | `backlog` |
| AX-20 | Future scatter-gather boundary | One turn legitimately wants both automation and baseline conversation | Current behavior stays deterministic; note future multi-handler routing work | `documented` |
