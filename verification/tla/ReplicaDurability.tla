-------------------------- MODULE ReplicaDurability --------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS MaxRevision, MaxGeneration, FenceDataFailure, OwnFailures, BarrierRetirement
VARIABLES received, pending, startedGeneration, generation, connected,
          saved, certified, staleFailureFenced, retired
vars == <<received, pending, startedGeneration, generation, connected,
          saved, certified, staleFailureFenced, retired>>
Init == /\ received = 0 /\ pending = 0 /\ startedGeneration = 0
        /\ generation = 0 /\ connected = TRUE /\ saved = {}
        /\ certified = 0 /\ staleFailureFenced = FALSE /\ retired = FALSE
BeginData == /\ connected /\ pending = 0 /\ received < MaxRevision
             /\ pending' = received + 1 /\ received' = received + 1
             /\ startedGeneration' = generation
             /\ UNCHANGED <<generation, connected, saved, certified, staleFailureFenced, retired>>
FinishData(ok) ==
  /\ pending # 0
  /\ pending' = 0
  /\ saved' = IF ok THEN saved \cup {pending} ELSE saved
  /\ retired' = (retired \/ (ok /\ pending = 1 /\ ~BarrierRetirement))
  /\ connected' = IF ~ok /\ FenceDataFailure
                       /\ (OwnFailures => startedGeneration = generation)
                   THEN FALSE ELSE connected
  /\ staleFailureFenced' = (staleFailureFenced
       \/ (~ok /\ FenceDataFailure /\ ~OwnFailures
           /\ startedGeneration # generation /\ connected))
  /\ UNCHANGED <<received, startedGeneration, generation, certified>>
Watermark(r) == /\ connected /\ pending = 0 /\ r \in 0..received
                /\ certified' = IF r > certified THEN r ELSE certified
                /\ UNCHANGED <<received, pending, startedGeneration, generation,
                               connected, saved, staleFailureFenced, retired>>
\* Replacement may occur while a write is awaiting storage. Reopen the same
\* partition from the last certified checkpoint, not the received wire position.
Replace == /\ generation < MaxGeneration
           /\ generation' = generation + 1 /\ received' = certified
           /\ connected' = TRUE
           /\ UNCHANGED <<pending, startedGeneration, saved, certified, staleFailureFenced, retired>>
Disconnect == /\ connected /\ connected' = FALSE
              /\ UNCHANGED <<received, pending, startedGeneration, generation,
                             saved, certified, staleFailureFenced, retired>>
\* A failed metadata-only write changes neither the certificate nor data.
MetadataFailure == UNCHANGED vars
\* One queued command has a known server receipt for data position one.
Retire == /\ ~retired /\ certified >= 1 /\ retired' = TRUE
          /\ UNCHANGED <<received, pending, startedGeneration, generation,
                         connected, saved, certified, staleFailureFenced>>
Next == Retire \/ BeginData \/ FinishData(TRUE) \/ FinishData(FALSE)
        \/ (\E r \in 0..MaxRevision: Watermark(r)) \/ Replace
        \/ Disconnect \/ MetadataFailure
TypeOK == /\ received \in 0..MaxRevision /\ pending \in 0..MaxRevision
          /\ certified \in 0..MaxRevision /\ saved \subseteq 1..MaxRevision
          /\ generation \in 0..MaxGeneration
          /\ startedGeneration \in 0..MaxGeneration
          /\ connected \in BOOLEAN /\ staleFailureFenced \in BOOLEAN /\ retired \in BOOLEAN
CertificateSafety == (1..certified) \subseteq saved
OldFailureIsolation == ~staleFailureFenced
RetirementSafety == retired => certified >= 1
=============================================================================
