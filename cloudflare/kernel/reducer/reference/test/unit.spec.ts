// The Rust unit tests of reference_integrity.rs and reference_integrity/jsonpath.rs,
// plus the serde_json behaviors the port reproduces.
import { describe, expect, it } from "vitest";
import { bindingId, configuration, decode, equal, formatFloat, JsonPath, parseJson, stringify, type Json } from "../index.ts";

const json = (text: string): Json => parseJson(text);
const texts = (values: Json[]) => values.map(stringify);

describe("reference_integrity.rs unit tests", () => {
  it("selector_decoders_match_their_declared_consumers", () => {
    expect(texts(decode(json('" [ 1, \\"selected\\", \\" \\" ] "'), "csv-or-json-list"))).toEqual(['"1"', '"selected"']);
    expect(texts(decode(json('" selected, other ,, "'), "csv-or-json-list"))).toEqual(['"selected"', '"other"']);
    expect(texts(decode(json('{"taskId":"selected","label":"A label"}'), "task-relation"))).toEqual(['"selected"']);
    expect(() => decode(json('[{"nested":1}]'), "csv-or-json-list")).toThrow("reference integrity: selector list needs scalar values");
    expect(decode(null, "task-relation")).toEqual([]);
  });

  it("binding_ids_and_configurations_are_stable", () => {
    expect(bindingId("manual", 3n)).toBe('["manual",3]');
    expect(bindingId("absent", null)).toBe('["absent",null]');
    expect(equal(configuration(json('{"options":{"a":1},"label":"x"}'), ["options"]), json('{"options":{"a":1}}'))).toBe(true);
  });
});

describe("jsonpath.rs unit tests", () => {
  const query = (path: string, document: string) => texts(JsonPath.parse(path).query(json(document)));

  it("lax_accessors_unwrap_and_wrap", () => {
    expect(query("$.teams[*]", '{"teams":["a","b"]}')).toEqual(['"a"', '"b"']);
    expect(query("$.selected[*]", '{"selected":"x"}')).toEqual(['"x"']);
    expect(query("$.missing", "{}")).toEqual([]);
    expect(query("$.items.id", '{"items":[{"id":1},{"id":2}]}')).toEqual(["1", "2"]);
    expect(query('$."payload".first', '{"payload":{"first":"one"}}')).toEqual(['"one"']);
    expect(query("$[0 to 1]", "[1, 2, 3]")).toEqual(["1", "2"]);
  });

  it("filters_methods_and_variables", () => {
    expect(query("$.value[*] ? (exists(@.fieldId))", '{"value":[{"fieldId":"a","selected":"x"},{"other":1}]}')).toEqual([
      '{"fieldId":"a","selected":"x"}',
    ]);
    const path = '$.value ? (@.type() == "object").keyvalue() ? (@.value.kind == "reference")';
    expect(query(path, '{"value":{"place":{"kind":"reference","refId":"t"},"label":"x"}}')).toEqual([
      '{"key":"place","value":{"kind":"reference","refId":"t"}}',
    ]);
    const vars = json('{"entry":{"key":"place"}}') as Map<string, Json>;
    const configurationDocument = json('{"attributes":[{"id":"place","reference":"teams"},{"id":"x","reference":"y"}]}');
    expect(JsonPath.parse("$.attributes[*] ? (@.id == $entry.key).reference").queryFirst(configurationDocument, vars)).toBe("teams");
  });

  it("predicates_follow_jsonb_path_match", () => {
    const live = JsonPath.parse("$.deletedAt == null");
    expect(live.matches(json('{"deletedAt":null}'))).toBe(true);
    expect(live.matches(json('{"deletedAt":1}'))).toBe(false);
    expect(live.matches(json("{}"))).toBe(false);
    expect(JsonPath.parse("true").matches(json("{}"))).toBe(true);
    const entity = JsonPath.parse('$.options.source == "entity"');
    expect(entity.matches(json('{"options":{"source":"entity"}}'))).toBe(true);
    expect(entity.matches(json('{"options":{"source":1}}'))).toBe(null);
    expect(() => JsonPath.parse("$.a ? (")).toThrow();
    expect(() => JsonPath.parse("$.a.unknown()")).toThrow("unsupported JSON path method unknown()");
    expect(JsonPath.parse("$.payload.first").rootMember()).toBe("payload");
  });

  it("parse errors keep Rust's texts", () => {
    expect(() => JsonPath.parse("$.a ? (")).toThrow("unexpected None in JSON path");
    expect(() => JsonPath.parse("$#")).toThrow(`unexpected '#' in JSON path "$#"`);
    expect(() => JsonPath.parse("$.a ? (@.b == 1")).toThrow("expected CloseParen in JSON path, found None");
    expect(() => JsonPath.parse("$[1.5]")).toThrow("invalid subscript 1.5");
    expect(() => JsonPath.parse("$.a)")).toThrow('unexpected input in JSON path "$.a)"');
    expect(() => JsonPath.parse("")).toThrow("unexpected None in JSON path");
    expect(() => JsonPath.parse("$[")).toThrow("expected a subscript, found None");
    expect(() => JsonPath.parse("$ ? 1")).toThrow("expected OpenParen in JSON path, found Some(Number(Number(1)))");
  });
});

describe("serde_json model", () => {
  it("prints floats as zmij does", () => {
    const cases: [number, string][] = [
      [6.62607015e-34, "6.62607015e-34"],
      [5.444310685350916e14, "544431068535091.6"],
      [5e-324, "5e-324"],
      [1.2e-322, "1.2e-322"],
      [1, "1.0"],
      [0, "0.0"],
      [-0, "-0.0"],
      [43210, "43210.0"],
      [43210.1, "43210.1"],
      [-5942736479622170, "-5942736479622170.0"],
      [3.439070283483335e35, "3.439070283483335e+35"],
      [2.9802322387695312e-8, "2.9802322387695312e-8"],
      [5.960464477539062e-7, "5.960464477539062e-7"],
      [1e16, "1e+16"],
      [0.00001, "0.00001"],
      [0.1, "0.1"],
    ];
    for (const [value, text] of cases) expect(formatFloat(value), String(value)).toBe(text);
  });

  it("keeps integers, floats and member order apart", () => {
    expect(stringify(json('{"b":1,"0":2.0,"a":[18446744073709551615,-9223372036854775808,18446744073709551616,-0]}'))).toBe(
      '{"b":1,"0":2.0,"a":[18446744073709551615,-9223372036854775808,1.8446744073709552e+19,-0.0]}',
    );
    expect(equal(json("1"), json("1.0"))).toBe(false);
    expect(equal(json('{"a":1,"b":2}'), json('{"b":2,"a":1}'))).toBe(true);
    expect(stringify(json('"\\u00e9\\n\\u0001\\ud83d\\ude00"'))).toBe('"é\\n\\u0001😀"');
  });

  it("reports serde_json parse errors", () => {
    expect(() => parseJson("")).toThrow("EOF while parsing a value at line 1 column 0");
    expect(() => parseJson("x")).toThrow("expected value at line 1 column 1");
    expect(() => parseJson("not-json")).toThrow("expected ident at line 1 column 2");
    expect(() => parseJson("[1,]")).toThrow("trailing comma at line 1 column 4");
    expect(() => parseJson("{} x")).toThrow("trailing characters at line 1 column 4");
    expect(() => parseJson('{"a":1')).toThrow("EOF while parsing an object at line 1 column 6");
  });
});
