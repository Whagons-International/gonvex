// Proof-only definitions and lemmas. No executable substitute serializer.
spec fn digit(n: int) -> char { (48 + n) as char }
spec fn decimal(n: nat) -> Seq<char>
    decreases n
{
    if n < 10 { seq![digit(n as int)] }
    else { decimal((n / 10) as nat) + seq![digit((n % 10) as int)] }
}
spec fn tokens(oids: Seq<u32>) -> Seq<char>
    decreases oids.len()
{
    if oids.len() == 0 { Seq::empty() }
    else { decimal(oids[0] as nat) + seq![','] + tokens(oids.drop_first()) }
}
spec fn execution(sql: Seq<char>, oids: Seq<u32>) -> Seq<char> {
    "/* gonvex.parameter-types.v1:"@ + tokens(oids) + " */\n"@ + sql
}
spec fn description_spec(sql: Seq<char>) -> Seq<char> {
    "/* gonvex.description.v1 */\n"@ + sql
}
spec fn excludes(s: Seq<char>, c: char) -> bool {
    forall|i: int| 0 <= i < s.len() ==> s[i] != c
}
proof fn decimal_facts(n: nat)
    ensures decimal(n).len() >= 1,
        (decimal(n).len() == 1) == (n < 10),
        forall|i: int| 0 <= i < decimal(n).len() ==>
            '0' <= #[trigger] decimal(n)[i] <= '9',
        excludes(decimal(n), ','), excludes(decimal(n), ' '),
    decreases n
{
    if n >= 10 {
        decimal_facts((n / 10) as nat);
    }
    assert(0 <= n % 10 < 10);
    assert forall|i: int| 0 <= i < decimal(n).len() implies
        '0' <= #[trigger] decimal(n)[i] <= '9' by {
        if n >= 10 && i < decimal((n / 10) as nat).len() { }
    }
}
proof fn decimal_injective(a: nat, b: nat)
    requires decimal(a) == decimal(b)
    ensures a == b
    decreases a + b
{
    decimal_facts(a);
    decimal_facts(b);
    if a < 10 {
        assert(b < 10);
        assert(decimal(a) == seq![digit(a as int)]);
        assert(decimal(b) == seq![digit(b as int)]);
        assert(decimal(a)[0] == decimal(b)[0]);
        assert(seq![digit(a as int)][0] == digit(a as int));
        assert(seq![digit(b as int)][0] == digit(b as int));
        assert(digit(a as int) == digit(b as int));
        assert(digit(a as int) as int == digit(b as int) as int);
        assert(digit(a as int) as int == 48 + a);
        assert(digit(b as int) as int == 48 + b);
        assert(a == b);
    } else {
        assert(b >= 10);
        let x = decimal((a / 10) as nat);
        let y = decimal((b / 10) as nat);
            assert(x.len() == y.len());
        assert(decimal(a)[x.len() as int] == decimal(b)[y.len() as int]);
        assert(digit((a % 10) as int) as int == 48 + a % 10);
        assert(digit((b % 10) as int) as int == 48 + b % 10);
        assert(decimal(a)[x.len() as int] == digit((a % 10) as int));
        assert(decimal(b)[y.len() as int] == digit((b % 10) as int));
        assert(digit((a % 10) as int) == digit((b % 10) as int));
        assert(a % 10 == b % 10);
        assert(x =~= y) by {
            assert forall|i: int| 0 <= i < x.len() implies x[i] == y[i] by {
                assert(decimal(a)[i] == x[i]);
                assert(decimal(b)[i] == y[i]);
            }
        }
        decimal_injective((a / 10) as nat, (b / 10) as nat);
        assert(a == (a / 10) * 10 + a % 10) by (nonlinear_arith);
        assert(b == (b / 10) * 10 + b % 10) by (nonlinear_arith);
        assert(a / 10 == b / 10);
        assert(a == b);
    }
}
// If c never appears in either header, its first occurrence fixes both lengths.
proof fn framed(a: Seq<char>, b: Seq<char>, x: Seq<char>, y: Seq<char>, c: char)
    requires excludes(a, c), excludes(b, c),
        a + seq![c] + x == b + seq![c] + y,
    ensures a == b, x == y
{
    if a.len() < b.len() {
        assert((a + seq![c] + x)[a.len() as int] == c);
        assert((b + seq![c] + y)[a.len() as int] == b[a.len() as int]);
    }
    if b.len() < a.len() {
        assert((b + seq![c] + y)[b.len() as int] == c);
        assert((a + seq![c] + x)[b.len() as int] == a[b.len() as int]);
    }
    assert(a.len() == b.len());
    assert(a =~= b) by {
        assert forall|i: int| 0 <= i < a.len() implies a[i] == b[i] by {
            assert((a + seq![c] + x)[i] == a[i]);
            assert((b + seq![c] + y)[i] == b[i]);
        }
    }
    assert((a + seq![c] + x).len() == a.len() + 1 + x.len());
    assert((b + seq![c] + y).len() == b.len() + 1 + y.len());
    assert(x.len() == y.len());
    assert(x =~= y) by {
        assert forall|i: int| 0 <= i < x.len() implies x[i] == y[i] by {
            assert((a + seq![c] + x)[a.len() + 1 + i] == x[i]);
            assert((b + seq![c] + y)[b.len() + 1 + i] == y[i]);
        }
    }
}
proof fn tokens_facts(oids: Seq<u32>)
    ensures excludes(tokens(oids), ' '),
        (tokens(oids).len() == 0) == (oids.len() == 0)
    decreases oids.len()
{
    if oids.len() > 0 {
        decimal_facts(oids[0] as nat);
        tokens_facts(oids.drop_first());
        assert forall|i: int| 0 <= i < tokens(oids).len() implies
            tokens(oids)[i] != ' ' by {
            let d = decimal(oids[0] as nat);
            if i < d.len() { }
            else if i > d.len() { }
        }
    }
}
proof fn tokens_injective(a: Seq<u32>, b: Seq<u32>)
    requires tokens(a) == tokens(b)
    ensures a == b
    decreases a.len() + b.len()
{
    tokens_facts(a);
    tokens_facts(b);
    if a.len() > 0 {
        assert(b.len() > 0);
        decimal_facts(a[0] as nat);
        decimal_facts(b[0] as nat);
        framed(decimal(a[0] as nat), decimal(b[0] as nat),
            tokens(a.drop_first()), tokens(b.drop_first()), ',');
        decimal_injective(a[0] as nat, b[0] as nat);
        tokens_injective(a.drop_first(), b.drop_first());
        assert(a =~= b) by {
            assert forall|i: int| 0 <= i < a.len() implies a[i] == b[i] by {
                if i > 0 {
                    assert(a.drop_first()[i - 1] == a[i]);
                    assert(b.drop_first()[i - 1] == b[i]);
                }
            }
        }
    } else { assert(a =~= b); }
}
proof fn tokens_push(a: Seq<u32>, n: u32)
    ensures tokens(a.push(n)) == tokens(a) + decimal(n as nat) + seq![',']
    decreases a.len()
{
    reveal_with_fuel(tokens, 3);
    if a.len() == 0 {
        assert(a.push(n)[0] == n);
        assert(a.push(n).drop_first() =~= Seq::<u32>::empty());
    } else {
        assert(a.push(n)[0] == a[0]);
        assert(a.push(n).drop_first() =~= a.drop_first().push(n));
        tokens_push(a.drop_first(), n);
        let head = decimal(a[0] as nat) + seq![','];
        let rest = tokens(a.drop_first());
        let last = decimal(n as nat) + seq![','];
        assert(tokens(a.push(n)) == head + (rest + last));
        assert(tokens(a) == head + rest);
        assert(head + (rest + last) =~= (head + rest) + last);
        assert(tokens(a) + last =~= tokens(a) + decimal(n as nat) + seq![',']);
    }
}
proof fn execution_identity(s: Seq<char>, t: Seq<char>, a: Seq<u32>, b: Seq<u32>)
    ensures (execution(s, a) == execution(t, b)) <==> (s == t && a == b)
{
    if execution(s, a) == execution(t, b) {
        let p = "/* gonvex.parameter-types.v1:"@;
        assert(execution(s, a) =~= p + (tokens(a) + " */\n"@ + s));
        assert(execution(t, b) =~= p + (tokens(b) + " */\n"@ + t));
        p.lemma_concat_injective(tokens(a) + " */\n"@ + s, tokens(b) + " */\n"@ + t);
        tokens_facts(a);
        tokens_facts(b);
        reveal_strlit(" */\n");
        assert(" */\n"@ =~= seq![' '] + "*/\n"@) by {
            reveal_strlit("*/\n");
        }
        assert(tokens(a) + " */\n"@ + s =~= tokens(a) + seq![' '] + ("*/\n"@ + s));
        assert(tokens(b) + " */\n"@ + t =~= tokens(b) + seq![' '] + ("*/\n"@ + t));
        framed(tokens(a), tokens(b), "*/\n"@ + s, "*/\n"@ + t, ' ');
        tokens_injective(a, b);
        "*/\n"@.lemma_concat_injective(s, t);
    }
}
proof fn description_identity(s: Seq<char>, t: Seq<char>)
    ensures (description_spec(s) == description_spec(t)) <==> (s == t)
{
    "/* gonvex.description.v1 */\n"@.lemma_concat_injective(s, t);
}
proof fn namespace_separation(s: Seq<char>, t: Seq<char>, a: Seq<u32>)
    ensures execution(s, a) != description_spec(t)
{
    reveal_strlit("/* gonvex.parameter-types.v1:");
    reveal_strlit("/* gonvex.description.v1 */\n");
    assert(execution(s, a)[10] == 'p');
    assert(description_spec(t)[10] == 'd');
}

spec fn decimal_value(chars: Seq<char>) -> nat
    decreases chars.len()
{
    if chars.len() == 0 { 0 }
    else { 10 * decimal_value(chars.drop_last()) + (chars.last() as int - 48) as nat }
}
proof fn decimal_value_inverse(n: nat)
    ensures decimal_value(decimal(n)) == n
    decreases n
{
    decimal_facts(n);
    reveal_with_fuel(decimal_value, 2);
    if n < 10 {
        assert(decimal(n).drop_last() =~= Seq::<char>::empty());
        assert(decimal(n).last() == digit(n as int));
        assert(digit(n as int) as int == 48 + n);
        assert(decimal_value(decimal(n)) == n);
    } else {
        let prefix = decimal((n / 10) as nat);
        assert(decimal(n).drop_last() =~= prefix);
        assert(decimal(n).last() == digit((n % 10) as int));
        decimal_value_inverse((n / 10) as nat);
        assert(n == (n / 10) * 10 + n % 10) by (nonlinear_arith);
        assert(digit((n % 10) as int) as int == 48 + n % 10);
        assert(decimal_value(decimal(n)) == 10 * decimal_value(prefix) + n % 10);
        assert(decimal_value(decimal(n)) == n);
    }
}
