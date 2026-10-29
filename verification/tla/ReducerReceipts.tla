-------------------------- MODULE ReducerReceipts -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS MaxClock, ExclusiveClaims, AtomicReceipt, ReplayHead, AllowNewTransport, AllowPruning
Actors == 1..2
VARIABLES phase, claims, receipt, receiptRevision, effects, clock, retained,
          barriers, answers, replied
vars == <<phase, claims, receipt, receiptRevision, effects, clock, retained,
          barriers, answers, replied>>
Init == /\ phase = [a \in Actors |-> "idle"] /\ claims = {}
        /\ receipt = 0 /\ receiptRevision = 0 /\ effects = 0 /\ clock = 0
        /\ retained = TRUE /\ barriers = [a \in Actors |-> 0]
        /\ answers = [a \in Actors |-> 0] /\ replied = {}
Acquire(a) == /\ phase[a] \in {"idle", "rolledBack"} /\ receipt = 0
              /\ a \notin claims /\ (ExclusiveClaims => claims = {})
              /\ claims' = claims \cup {a}
              /\ phase' = [phase EXCEPT ![a] = "claimed"]
              /\ UNCHANGED <<receipt, receiptRevision, effects, clock, retained,
                             barriers, answers, replied>>
Prepare(a) == /\ phase[a] = "claimed" /\ clock < MaxClock
              /\ phase' = [phase EXCEPT ![a] = "prepared"]
              /\ receipt' = IF AtomicReceipt THEN receipt ELSE a
              /\ receiptRevision' = IF AtomicReceipt THEN receiptRevision ELSE clock + 1
              /\ UNCHANGED <<claims, effects, clock, retained, barriers, answers, replied>>
Commit(a) == /\ phase[a] = "prepared" /\ a \in claims /\ clock < MaxClock
             /\ claims' = claims \ {a} /\ effects' = effects + 1
             /\ receipt' = a /\ receiptRevision' = clock + 1 /\ clock' = clock + 1
             /\ phase' = [phase EXCEPT ![a] = "committed"]
             /\ barriers' = [barriers EXCEPT ![a] = clock + 1]
             /\ answers' = [answers EXCEPT ![a] = a]
             /\ retained' = TRUE /\ UNCHANGED replied
Rollback(a) == /\ phase[a] \in {"claimed", "prepared"}
               /\ claims' = claims \ {a}
               /\ phase' = [phase EXCEPT ![a] = "rolledBack"]
               /\ UNCHANGED <<receipt, receiptRevision, effects, clock, retained,
                              barriers, answers, replied>>
Replay(a) == /\ receipt # 0 /\ (AllowNewTransport \/ a = receipt) /\ phase[a] \in {"idle", "rolledBack", "committed", "replied"}
             /\ phase' = [phase EXCEPT ![a] = "replayed"]
             /\ barriers' = [barriers EXCEPT ![a] = IF ReplayHead THEN clock
                         ELSE IF retained /\ a = receipt THEN receiptRevision ELSE 0]
             /\ answers' = [answers EXCEPT ![a] = receipt]
             /\ replied' = replied \ {a}
             /\ UNCHANGED <<claims, receipt, receiptRevision, effects, clock, retained>>
Reply(a) == /\ phase[a] \in {"committed", "replayed"}
            /\ phase' = [phase EXCEPT ![a] = "replied"]
            /\ replied' = replied \cup {a}
            /\ UNCHANGED <<claims, receipt, receiptRevision, effects, clock, retained,
                           barriers, answers>>
Prune == /\ AllowPruning /\ retained /\ retained' = FALSE
         /\ UNCHANGED <<phase, claims, receipt, receiptRevision, effects, clock,
                        barriers, answers, replied>>
OtherCommit == /\ clock < MaxClock /\ clock' = clock + 1
               /\ UNCHANGED <<phase, claims, receipt, receiptRevision, effects,
                              retained, barriers, answers, replied>>
Next == (\E a \in Actors: Acquire(a) \/ Prepare(a) \/ Commit(a) \/ Rollback(a)
                          \/ Replay(a) \/ Reply(a))
        \/ Prune \/ OtherCommit \/ UNCHANGED vars
TypeOK == /\ phase \in [Actors -> {"idle", "claimed", "prepared", "committed",
                                  "rolledBack", "replayed", "replied"}]
          /\ claims \subseteq Actors /\ receipt \in 0..2
          /\ receiptRevision \in 0..MaxClock /\ effects \in 0..2
          /\ clock \in 0..MaxClock /\ retained \in BOOLEAN
          /\ barriers \in [Actors -> 0..MaxClock]
          /\ answers \in [Actors -> 0..2] /\ replied \subseteq Actors
AtMostOnce == effects <= 1
ReceiptImpliesEffect == receipt # 0 => effects = 1
ReplyHasBarrier == \A a \in replied: barriers[a] >= receiptRevision
CachedResult == \A a \in replied: answers[a] = receipt
=============================================================================
