package mongo

import (
	"context"
)

// AIAnnotation is one field-level recommendation returned by an AI provider.
type AIAnnotation struct {
	FieldName  string      `json:"field_name"`
	Recommended FieldOption `json:"recommended"`
	Reasoning  string      `json:"reasoning"`
}

// AIAnnotator analyses an InferredSchema and returns per-field annotations.
// Implementations are optional; when no provider is configured the engine
// uses NoopAnnotator so callers never need to nil-check.
type AIAnnotator interface {
	Annotate(ctx context.Context, sample *CollectionSample, schema *InferredSchema) ([]AIAnnotation, error)
}

// NoopAnnotator is used when no AI provider key is configured.
// It returns empty annotations immediately so the rest of the pipeline
// is unaffected.
type NoopAnnotator struct{}

func (n *NoopAnnotator) Annotate(_ context.Context, _ *CollectionSample, _ *InferredSchema) ([]AIAnnotation, error) {
	return nil, nil
}

// ApplyAnnotations merges AI annotations back into an InferredSchema.
// Fields not mentioned by the AI are left unchanged. Annotations only add
// the AIReasoning string and optionally reorder Options so the AI recommendation
// appears first; they never remove options or override the plan row.
func ApplyAnnotations(schema *InferredSchema, annotations []AIAnnotation) {
	byField := make(map[string]AIAnnotation, len(annotations))
	for _, a := range annotations {
		byField[a.FieldName] = a
	}

	for i := range schema.Fields {
		ann, ok := byField[schema.Fields[i].Frequency.FieldName]
		if !ok {
			continue
		}
		// Prepend the AI-recommended option if it differs from the current first option.
		opts := schema.Fields[i].Options
		if len(opts) > 0 && (opts[0].PgType != ann.Recommended.PgType || opts[0].Strategy != ann.Recommended.Strategy) {
			// Move the AI recommendation to the front without duplicating it.
			filtered := make([]FieldOption, 0, len(opts))
			filtered = append(filtered, ann.Recommended)
			for _, o := range opts {
				if o.PgType != ann.Recommended.PgType || o.Strategy != ann.Recommended.Strategy {
					filtered = append(filtered, o)
				}
			}
			schema.Fields[i].Options = filtered
		}
		// Attach reasoning as a warning so it surfaces in API output.
		if ann.Reasoning != "" {
			schema.Fields[i].Warnings = append(schema.Fields[i].Warnings, "AI: "+ann.Reasoning)
		}
	}
}

// NewAnnotator returns the appropriate AIAnnotator based on available
// configuration. Currently only NoopAnnotator is implemented; concrete
// providers (Anthropic, OpenAI) will be wired here when API keys are
// detected in the server config.
func NewAnnotator() AIAnnotator {
	return &NoopAnnotator{}
}
