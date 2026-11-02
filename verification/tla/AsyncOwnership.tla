-------------------------- MODULE AsyncOwnership --------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS MaxGeneration, Policy
VARIABLES owner, generation, closed, stage, capturedOwner, capturedGeneration,
          admitted, journal, published
vars == <<owner, generation, closed, stage, capturedOwner, capturedGeneration,
          admitted, journal, published>>
Owners == {"alice", "bob"}
Owns == ~closed /\ owner = capturedOwner /\ generation = capturedGeneration
Guard(point) == IF Policy = "scope-only" THEN ~closed /\ owner = capturedOwner
                ELSE IF Policy = point THEN TRUE ELSE Owns
Observation == <<capturedOwner, capturedGeneration, owner, generation, closed>>
Init == /\ owner = "alice" /\ generation = 0 /\ closed = FALSE
        /\ stage = "read" /\ capturedOwner = owner
        /\ capturedGeneration = generation
        /\ admitted = {} /\ journal = {} /\ published = {}
Rotate(next) == /\ ~closed /\ generation < MaxGeneration /\ next \in Owners
                /\ owner' = next /\ generation' = generation + 1
                /\ UNCHANGED <<closed, stage, capturedOwner, capturedGeneration, admitted, journal, published>>
Close == /\ ~closed /\ closed' = TRUE
         /\ UNCHANGED <<owner, generation, stage, capturedOwner, capturedGeneration, admitted, journal, published>>
ReadCompletes == /\ stage = "read"
                 /\ stage' = IF Guard("omit-read") THEN "write" ELSE "done"
                 /\ admitted' = IF Guard("omit-read") THEN admitted \cup {Observation} ELSE admitted
                 /\ UNCHANGED <<owner, generation, closed, capturedOwner, capturedGeneration, journal, published>>
WriteCompletes ==
  /\ stage = "write"
  \* Completion of an already admitted write may outlive its activation.
  /\ journal' = journal \cup {<<capturedOwner, IF Policy = "reroute-journal" THEN owner ELSE capturedOwner>>}
  /\ IF Guard("omit-write")
        THEN \/ /\ stage' = "done" /\ published' = published \cup {Observation}
             \/ /\ stage' = "rebase" /\ UNCHANGED published
        ELSE /\ stage' = "done" /\ UNCHANGED published
  /\ UNCHANGED <<owner, generation, closed, capturedOwner, capturedGeneration, admitted>>
RebaseCompletes == /\ stage = "rebase" /\ stage' = "done"
                   /\ published' = IF Guard("omit-rebase") THEN published \cup {Observation} ELSE published
                   /\ UNCHANGED <<owner, generation, closed, capturedOwner, capturedGeneration, admitted, journal>>
Next == (\E next \in Owners : Rotate(next)) \/ Close \/ ReadCompletes
        \/ WriteCompletes \/ RebaseCompletes \/ UNCHANGED vars
TypeOK == /\ owner \in Owners /\ capturedOwner \in Owners
          /\ generation \in 0..MaxGeneration /\ capturedGeneration \in 0..MaxGeneration
          /\ closed \in BOOLEAN /\ stage \in {"read", "write", "rebase", "done"}
          /\ admitted \subseteq Owners \X (0..MaxGeneration) \X Owners \X (0..MaxGeneration) \X BOOLEAN
          /\ published \subseteq Owners \X (0..MaxGeneration) \X Owners \X (0..MaxGeneration) \X BOOLEAN
          /\ journal \subseteq Owners \X Owners
Valid(observation) == observation[1] = observation[3] /\ observation[2] = observation[4] /\ ~observation[5]
AdmissionSafety == \A observation \in admitted : Valid(observation)
PublicationSafety == \A observation \in published : Valid(observation)
JournalIsolation == \A write \in journal : write[1] = write[2]
NoSuccessfulPublication == published = {}
=============================================================================
