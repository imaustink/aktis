package facade

import (
	"encoding/json"
	"fmt"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// ParseCallerTools validates a request's raw tools/tool_choice (decoded JSON,
// any shape) and hands the well-typed remainder to callertools.Parse, which
// owns the shared rules (caps, name pattern, hashing). The loose-type checks
// here are the ones a struct decode cannot express, with the messages the
// chat facade has always returned.
func ParseCallerTools(rawTools, rawToolChoice any) ([]callertools.Descriptor, callertools.Choice, error) {
	choiceJSON, _ := json.Marshal(rawToolChoice)
	if rawToolChoice == nil {
		choiceJSON = nil
	}
	req := callertools.Request{ToolChoice: choiceJSON}

	if rawTools != nil {
		list, ok := rawTools.([]any)
		if !ok {
			return nil, callertools.Choice{}, fmt.Errorf("tools must be an array")
		}
		// tool_choice errors win over tools errors, as before.
		if _, _, err := callertools.Parse(callertools.Request{ToolChoice: choiceJSON}); err != nil {
			return nil, callertools.Choice{}, err
		}
		if len(list) > callertools.MaxTools {
			return nil, callertools.Choice{}, fmt.Errorf("tools may contain at most %d entries (received %d)", callertools.MaxTools, len(list))
		}
		for i, entry := range list {
			obj, ok := entry.(map[string]any)
			if !ok {
				return nil, callertools.Choice{}, fmt.Errorf("tools[%d] must be an object", i)
			}
			if t, present := obj["type"]; present && t != "function" {
				return nil, callertools.Choice{}, fmt.Errorf("tools[%d].type must be \"function\"", i)
			}
			fn, ok := obj["function"].(map[string]any)
			if !ok {
				return nil, callertools.Choice{}, fmt.Errorf("tools[%d].function must be an object", i)
			}
			if _, ok := fn["name"].(string); !ok {
				return nil, callertools.Choice{}, fmt.Errorf("tools[%d].function.name must be a non-empty string", i)
			}
			if d, present := fn["description"]; present && d != nil {
				if _, ok := d.(string); !ok {
					return nil, callertools.Choice{}, fmt.Errorf("tools[%d].function.description must be a string", i)
				}
			}
			if p, present := fn["parameters"]; present && p != nil {
				switch p.(type) {
				case map[string]any, []any:
				default:
					return nil, callertools.Choice{}, fmt.Errorf("tools[%d].function.parameters must be a JSON Schema object", i)
				}
			}
			description, _ := fn["description"].(string)
			var parameters json.RawMessage
			if p := fn["parameters"]; p != nil {
				parameters, _ = json.Marshal(p)
			}
			req.Tools = append(req.Tools, callertools.RawTool{
				Type: "function",
				Function: callertools.RawFunction{
					Name:        fn["name"].(string),
					Description: description,
					Parameters:  parameters,
				},
			})
		}
	}
	return callertools.Parse(req)
}
