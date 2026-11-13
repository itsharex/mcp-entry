package gateway

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func exactJSON(t *testing.T, raw string) any {
	t.Helper()
	value, err := decodeExactJSON([]byte(raw), 32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestExactJSONBoundsAndPrecision(t *testing.T) {
	for _, raw := range []string{`0`, `-0`, `0.1`, `1.00000000000000001`, `9007199254740993`, `18446744073709551615`, `1e308`, `1e-4096`, `0e4096`} {
		t.Run(raw, func(t *testing.T) {
			value := exactJSON(t, raw)
			if value != json.Number(raw) {
				t.Fatalf("number changed: %#v", value)
			}
		})
	}
	for _, raw := range []string{`1e309`, `-1e309`, `1e4097`, `1e-4097`, `0e9999999999999999999`, `NaN`, `1/2`, `1 2`, `01`, `1.`, "0." + strings.Repeat("0", 1023) + "1"} {
		if _, err := decodeExactJSON([]byte(raw), 32, 4096); err == nil {
			t.Fatalf("unsupported number/JSON accepted: %s", raw)
		}
	}
	for _, tc := range []struct {
		raw          string
		depth, nodes int
	}{{`[[[1]]]`, 2, 10}, {`[1,2]`, 4, 2}, {`{"annotation":[1e999999999]}`, 4, 10}} {
		if _, err := decodeExactJSON([]byte(tc.raw), tc.depth, tc.nodes); err == nil {
			t.Fatal("budget exceeded without rejection")
		}
	}
}

func TestExactSchemaNumericSemantics(t *testing.T) {
	cases := []struct {
		schema, instance string
		valid            bool
	}{
		{`{"type":"integer"}`, `1.00000000000000001`, false},
		{`{"type":"integer"}`, `9007199254740992.5`, false},
		{`{"type":"integer"}`, `9007199254740993`, true},
		{`{"type":"number"}`, `1e-4096`, true},
		{`{"minimum":9007199254740993}`, `9007199254740992`, false},
		{`{"maximum":9007199254740993}`, `9007199254740993`, true},
		{`{"exclusiveMinimum":1.00000000000000001}`, `1.00000000000000002`, true},
		{`{"exclusiveMaximum":1.00000000000000001}`, `1.00000000000000001`, false},
		{`{"enum":[9007199254740993]}`, `9007199254740992`, false},
		{`{"const":1.00000000000000001}`, `1`, false},
		{`{"multipleOf":3}`, `1e20`, false},
		{`{"multipleOf":0.1}`, `0.3`, true},
		{`{"multipleOf":0.1}`, `0.30000000000000001`, false},
		{`{"not":{"type":"integer"}}`, `1.00000000000000001`, true},
		{`{"anyOf":[{"const":1.00000000000000001},{"const":2}]}`, `1`, false},
		{`{"oneOf":[{"type":"integer"},{"type":"number"}]}`, `1.00000000000000001`, true},
		{`{"type":"array","items":{"type":"integer"}}`, `[1.00000000000000001]`, false},
		{`{"type":"array","uniqueItems":true}`, `[9007199254740992,9007199254740993]`, true},
		{`{"type":"array","uniqueItems":true}`, `[1,1.0]`, false},
		{`{"if":{"type":"integer"},"then":{"const":2},"else":{"const":1.00000000000000001}}`, `1.00000000000000001`, true},
	}
	for _, tc := range cases {
		t.Run(tc.schema+"/"+tc.instance, func(t *testing.T) {
			schema, err := compileExactSchema(exactJSON(t, tc.schema), false)
			if err != nil {
				t.Fatal(err)
			}
			if valid := schema.Validate(exactJSON(t, tc.instance)) == nil; valid != tc.valid {
				t.Fatalf("valid=%v want %v", valid, tc.valid)
			}
		})
	}
}

func TestExactSchemaCompatibilityAndImmutability(t *testing.T) {
	raw := `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","definitions":{"v":{"type":"string","format":"regex"}},"properties":{"x":{"$ref":"#/definitions/v"},"format":{"const":{"format":"unchanged","$schema":"unchanged"}},"email":{"type":"string","format":"email"}},"default":{"format":"unchanged"},"examples":[{"format":"unchanged"}]}`
	value := exactJSON(t, raw)
	before, _ := json.Marshal(value)
	schema, err := compileExactSchema(value, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(exactJSON(t, `{"x":"[","email":"not-email","format":{"format":"unchanged","$schema":"unchanged"}}`)); err != nil {
		t.Fatal("introduced format assertions or mutated const data", err)
	}
	after, _ := json.Marshal(value)
	if string(before) != string(after) {
		t.Fatal("advertised schema mutated")
	}
	for _, raw := range []string{`{"$schema":"","type":"object"}`, `{"$schema":"https://json-schema.org/draft-07/schema#","type":"object"}`} {
		if _, err := compileExactSchema(exactJSON(t, raw), true); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"type":["object"]}`, `true`, `{"type":"object","properties":{"x":{"format":42}}}`, `{"type":"object","$ref":"https://example.invalid/a"}`, `{"type":"object","multipleOf":0}`, `{"type":"object","minLength":-1}`, `{"type":"object","properties":{"x":{"$dynamicRef":"#"}}}`} {
		if _, err := compileExactSchema(exactJSON(t, raw), true); err == nil {
			t.Fatal("unsupported/invalid schema accepted", raw)
		}
	}
}

func TestExactSchemaReferencesAndDraftSemantics(t *testing.T) {
	cases := []struct {
		name, schema, instance string
		valid                  bool
	}{
		{"draft7 ignores ref siblings", `{"$schema":"http://json-schema.org/draft-07/schema#","definitions":{"v":{"type":"number"}},"properties":{"x":{"$ref":"#/definitions/v","type":"string"}}}`, `{"x":9007199254740993}`, true},
		{"2020 applies ref siblings", `{"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"v":{"type":"number"}},"properties":{"x":{"$ref":"#/$defs/v","type":"string"}}}`, `{"x":9007199254740993}`, false},
		{"encoded ref numeric boundary", `{"$defs":{"a/b":{"minimum":9007199254740993}},"properties":{"x":{"$ref":"#/$defs/a%7E1b"}}}`, `{"x":9007199254740992}`, false},
		{"patternProperties numeric boundary", `{"patternProperties":{"^n":{"type":"integer"}},"additionalProperties":false}`, `{"number":1.00000000000000001}`, false},
		{"patternProperties format compatibility", `{"$schema":"http://json-schema.org/draft-07/schema#","patternProperties":{"^n":{"type":"string","format":"email"}}}`, `{"name":"not-email"}`, true},
		{"tuple numeric boundary", `{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","items":[{"type":"integer"},{"multipleOf":0.1}],"additionalItems":false}`, `[9007199254740993,0.3]`, true},
		{"unevaluated property", `{"allOf":[{"properties":{"x":{"type":"integer"}}}],"unevaluatedProperties":false}`, `{"x":9007199254740993,"y":1}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := compileExactSchema(exactJSON(t, tc.schema), false)
			if err != nil {
				t.Fatal(err)
			}
			if valid := schema.Validate(exactJSON(t, tc.instance)) == nil; valid != tc.valid {
				t.Fatalf("valid=%v want %v", valid, tc.valid)
			}
		})
	}
}

func TestExactSchemaCountOverflow(t *testing.T) {
	for _, keyword := range []string{"minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains"} {
		for _, number := range []string{"18446744073709551616", "9223372036854775808", "1e100", "-1", "1.5"} {
			raw := fmt.Sprintf(`{"properties":{"nested":{"%s":%s}}}`, keyword, number)
			if _, err := compileExactSchema(exactJSON(t, raw), false); err == nil {
				t.Fatalf("count %s=%s was silently truncated or accepted", keyword, number)
			}
		}
	}
	// Count-like names inside enum/annotation objects remain literal data.
	if _, err := compileExactSchema(exactJSON(t, `{"const":{"minLength":18446744073709551616},"examples":[{"maxItems":18446744073709551616}]}`), false); err != nil {
		t.Fatal("count limits leaked into data", err)
	}
}

func TestExactSchemaRejectsUnbudgetedRecursiveDialect(t *testing.T) {
	for _, raw := range []string{
		`{"type":"object","properties":{"x":{"$id":"urn:child","$schema":"https://json-schema.org/draft/2019-09/schema","$recursiveAnchor":true,"type":"object","properties":{"next":{"$recursiveRef":"#"}}}}}`,
		`{"type":"object","properties":{"x":{"$id":"urn:child","$schema":"https://json-schema.org/draft/2019-09/schema"}}}`,
		`{"type":"object","properties":{"x":{"$recursiveRef":"#"}}}`,
		`{"type":"object","properties":{"x":{"$recursiveAnchor":true}}}`,
	} {
		if _, err := compileExactSchema(exactJSON(t, raw), true); err == nil {
			t.Fatalf("unbudgeted recursive/dialect schema accepted: %s", raw)
		}
	}
	// Annotation and const values are data, not subschemas.
	raw := `{"type":"object","const":{"$schema":"https://json-schema.org/draft/2019-09/schema","$recursiveRef":"#"},"examples":[{"$recursiveAnchor":true}]}`
	if _, err := compileExactSchema(exactJSON(t, raw), true); err != nil {
		t.Fatal("schema-position rule leaked into data", err)
	}
}
