// Proof-only definitions. Production executable statements are inserted by
// verify_snapshot_credit.py from snapshot_ack.rs, never reimplemented here.
spec fn byte_sum(records: Seq<(u64, usize)>) -> int
    decreases records.len(),
{
    if records.len() == 0 { 0 }
    else { records[0].1 as int + byte_sum(records.skip(1)) }
}

proof fn byte_sum_split(records: Seq<(u64, usize)>, cut: int)
    requires 0 <= cut <= records.len(),
    ensures byte_sum(records) == byte_sum(records.take(cut)) + byte_sum(records.skip(cut)),
    decreases cut,
{
    if cut == 0 {
        assert(records.take(0) =~= Seq::<(u64, usize)>::empty());
        assert(records.skip(0) =~= records);
    } else {
        byte_sum_split(records.skip(1), cut - 1);
        assert(records.take(cut).skip(1) =~= records.skip(1).take(cut - 1));
        assert(records.skip(cut) =~= records.skip(1).skip(cut - 1));
        assert(records.take(cut)[0] == records[0]);
    }
}
