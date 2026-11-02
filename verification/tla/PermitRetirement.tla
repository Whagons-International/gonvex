------------------------ MODULE PermitRetirement ------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Capacity, Policy
Workers == 1..(Capacity + 1)
Phases == {"absent", "starting", "leased", "idle", "retiring", "exited", "stopped"}
VARIABLES phase, held, available, completed
vars == <<phase, held, available, completed>>
Live == {w \in Workers : phase[w] \in {"starting", "leased", "idle", "retiring"}}
Idle == {w \in Workers : phase[w] = "idle"}
Init == /\ phase = [w \in Workers |-> "absent"]
        /\ held = {} /\ available = Capacity /\ completed = 0
Spawn(w) == /\ available > 0 /\ Idle = {}
            /\ phase[w] \in {"absent", "stopped"}
            /\ phase' = [phase EXCEPT ![w] = "starting"]
            /\ held' = held \cup {w} /\ available' = available - 1
            /\ UNCHANGED completed
LeaseIdle(w) == /\ available > 0 /\ phase[w] = "idle"
                /\ phase' = [phase EXCEPT ![w] = "leased"]
                /\ held' = held \cup {w} /\ available' = available - 1
                /\ UNCHANGED completed
Ready(w) == /\ phase[w] = "starting"
            /\ phase' = [phase EXCEPT ![w] = "leased"]
            /\ UNCHANGED <<held, available, completed>>
Healthy(w) == /\ phase[w] = "leased"
              /\ phase' = [phase EXCEPT ![w] = "idle"]
              /\ held' = held \ {w} /\ available' = available + 1
              /\ completed' = IF completed < 2 THEN completed + 1 ELSE 2
Retire(w) == /\ phase[w] \in {"starting", "leased"}
             /\ phase' = [phase EXCEPT ![w] = "retiring"]
             /\ IF Policy = "early-retire"
                   THEN /\ held' = held \ {w} /\ available' = available + 1
                   ELSE UNCHANGED <<held, available>>
             /\ UNCHANGED completed
NativeExit(w) == /\ phase[w] \in {"starting", "leased"}
                 /\ phase' = [phase EXCEPT ![w] = "exited"]
                 /\ UNCHANGED <<held, available, completed>>
RetireExited(w) == /\ phase[w] = "exited"
                   /\ phase' = [phase EXCEPT ![w] = "stopped"]
                   /\ held' = held \ {w} /\ available' = available + 1
                   /\ UNCHANGED completed
Dispose(w) == /\ phase[w] = "retiring"
              /\ phase' = [phase EXCEPT ![w] = "stopped"]
              /\ IF Policy = "leak-retire"
                    THEN UNCHANGED <<held, available>>
                    ELSE /\ held' = held \ {w}
                         /\ available' = available + (IF Policy = "double-stop" THEN 2 ELSE 1)
              /\ UNCHANGED completed
Next == \E w \in Workers : Spawn(w) \/ LeaseIdle(w) \/ Ready(w) \/ Healthy(w)
                         \/ Retire(w) \/ NativeExit(w) \/ RetireExited(w) \/ Dispose(w)
TypeOK == /\ phase \in [Workers -> Phases] /\ held \subseteq Workers
          /\ available \in Nat /\ completed \in 0..2
Accounting == available + Cardinality(held) = Capacity
LeaseOwnsPermit == \A w \in Workers : phase[w] \in {"starting", "leased", "exited"} => w \in held
RetirementRetainsCapacity == \A w \in Workers : phase[w] = "retiring" => w \in held
StoppedOwnsNoPermit == \A w \in Workers : phase[w] \in {"absent", "idle", "stopped"} => w \notin held
LiveCapacity == Cardinality(Live) <= Capacity
NoSecondCall == completed < 2
=============================================================================
