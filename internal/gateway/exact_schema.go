package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"

	exactschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Numeric precision is not a resource limit. Bound lexical size and exponent
// before any big.Rat construction, including numbers in annotation/enum data.
// The SDK's ordinary result decoder must also be able to receive the magnitude:
// overflow to infinity is unsupported, but underflow is retained exactly here.
// No float value produced by this admission check is used for validation.
const (
	MaxJSONNumberBytes    = 1024
	MaxJSONNumberExponent = 4096
)

var errExactJSON = errors.New("invalid or unsupported JSON value")

// decodeExactJSON preserves decimal tokens, requires exactly one JSON value,
// and checks structure and number budgets before a numeric evaluator sees it.
// The caller separately enforces its wire byte limit (arguments/schema/result).
func decodeExactJSON(data []byte, maxDepth, maxNodes int) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errExactJSON
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errExactJSON
	}
	if _, err := countJSONBudget(value, maxDepth, maxNodes); err != nil {
		return nil, errExactJSON
	}
	if err := checkExactNumbers(value); err != nil {
		return nil, err
	}
	return value, nil
}

// Only call after a structure/depth budget has been checked. Decoder.UseNumber
// guarantees that numeric values have JSON grammar, not arbitrary big.Rat text.
func checkExactNumbers(value any) error {
	switch value := value.(type) {
	case json.Number:
		text := string(value)
		if len(text) > MaxJSONNumberBytes {
			return errExactJSON
		}
		if i := strings.IndexAny(text, "eE"); i >= 0 {
			exponent, err := strconv.ParseInt(text[i+1:], 10, 32)
			if err != nil || exponent < -MaxJSONNumberExponent || exponent > MaxJSONNumberExponent {
				return errExactJSON
			}
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return errExactJSON
		}
	case map[string]any:
		for _, child := range value {
			if err := checkExactNumbers(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := checkExactNumbers(child); err != nil {
				return err
			}
		}
	}
	return nil
}

type deniedSchemaLoader struct{}

func (deniedSchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external schema loading disabled")
}

// compileExactSchema keeps the existing bounded schema vocabulary and dialect
// policy. It never changes the advertised schema. The private compilation copy
// removes only historical no-op format annotations and empty dialect markers;
// this avoids silently introducing draft-07 format assertions during the numeric
// engine change. The new engine rejects malformed schemas more strictly.
func compileExactSchema(value any, input bool) (*exactschema.Schema, error) {
	if _, err := analyzeSchema(value); err != nil {
		return nil, err
	}
	if err := validateSchemaTypes(value); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil || len(data) > 64<<10 {
		return nil, errExactJSON
	}
	copy, err := decodeExactJSON(data, MaxSchemaDepth, MaxSchemaRawNodes)
	if err != nil {
		return nil, err
	}
	root, object := copy.(map[string]any)
	if input && (!object || root["type"] != "object") {
		return nil, errors.New("input schema must describe object")
	}
	if err := prepareExactSchemaCopy(copy); err != nil {
		return nil, err
	}
	compiler := exactschema.NewCompiler()
	compiler.DefaultDraft(exactschema.Draft2020)
	compiler.UseLoader(deniedSchemaLoader{})
	const resource = "urn:mcp-entry:input-schema"
	if err := compiler.AddResource(resource, copy); err != nil {
		return nil, err
	}
	return compiler.Compile(resource)
}

// Walk schema positions only. A property named "format", const/default/example
// payload or unknown extension is data, and must not be altered. Full raw JSON
// cloning already happened through decodeExactJSON above.
func prepareExactSchemaCopy(value any) error {
	schema, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if format, exists := schema["format"]; exists && format != nil {
		if _, ok := format.(string); !ok {
			return errors.New("invalid format annotation")
		}
	}
	// The pinned engine stores cardinalities in int and otherwise truncates
	// big.Int.Int64 silently. Reject unsupported counts before compilation;
	// this does not constrain minimum/maximum/multipleOf numeric values.
	maxCount := new(big.Int).SetUint64(uint64(^uint(0) >> 1))
	for _, key := range []string{"minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains"} {
		if number, ok := schema[key].(json.Number); ok {
			count, valid := new(big.Rat).SetString(string(number))
			if !valid || !count.IsInt() || count.Sign() < 0 || count.Num().Cmp(maxCount) > 0 {
				return errors.New("invalid or unsupported schema count")
			}
		}
	}
	delete(schema, "format")
	if schema["$schema"] == "" {
		delete(schema, "$schema")
	}
	for key, child := range schema {
		switch key {
		case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas", "dependencies":
			if children, ok := child.(map[string]any); ok {
				for _, child := range children {
					if err := prepareExactSchemaCopy(child); err != nil {
						return err
					}
				}
			}
		case "allOf", "anyOf", "oneOf", "prefixItems":
			if children, ok := child.([]any); ok {
				for _, child := range children {
					if err := prepareExactSchemaCopy(child); err != nil {
						return err
					}
				}
			}
		case "items":
			if children, ok := child.([]any); ok {
				for _, child := range children {
					if err := prepareExactSchemaCopy(child); err != nil {
						return err
					}
				}
			} else {
				if err := prepareExactSchemaCopy(child); err != nil {
					return err
				}
			}
		case "additionalProperties", "additionalItems", "propertyNames", "contains", "not", "if", "then", "else", "unevaluatedProperties", "unevaluatedItems", "contentSchema":
			if err := prepareExactSchemaCopy(child); err != nil {
				return err
			}
		}
	}
	return nil
}
