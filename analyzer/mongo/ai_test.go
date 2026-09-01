package mongo

import (
	"context"
	"testing"
)

func TestNoopAnnotator_ReturnsEmpty(t *testing.T) {
	n := &NoopAnnotator{}
	anns, err := n.Annotate(context.Background(), &CollectionSample{}, &InferredSchema{})
	if err != nil {
		t.Fatalf("Annotate: %v", err)
	}
	if len(anns) != 0 {
		t.Errorf("NoopAnnotator returned %d annotations, want 0", len(anns))
	}
}

func TestApplyAnnotations_PrependsAIOption(t *testing.T) {
	schema := &InferredSchema{
		Fields: []InferredField{
			{
				Frequency: FieldFrequency{FieldName: "total"},
				Options: []FieldOption{
					{PgType: "double precision", Strategy: "direct"},
					{PgType: "numeric", Strategy: "direct"},
				},
			},
		},
	}

	annotations := []AIAnnotation{
		{
			FieldName:   "total",
			Recommended: FieldOption{PgType: "numeric", Strategy: "direct"},
			Reasoning:   "3% Decimal128 values suggest monetary data; numeric is exact",
		},
	}

	ApplyAnnotations(schema, annotations)

	opts := schema.Fields[0].Options
	if opts[0].PgType != "numeric" {
		t.Errorf("first option after AI = %q, want \"numeric\"", opts[0].PgType)
	}
	// Original first option still present, not duplicated.
	count := 0
	for _, o := range opts {
		if o.PgType == "numeric" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("numeric option appears %d times, want exactly 1", count)
	}
}

func TestApplyAnnotations_AttachesReasoning(t *testing.T) {
	schema := &InferredSchema{
		Fields: []InferredField{
			{
				Frequency: FieldFrequency{FieldName: "f"},
				Options:   []FieldOption{{PgType: "text", Strategy: "direct"}},
			},
		},
	}
	ApplyAnnotations(schema, []AIAnnotation{
		{FieldName: "f", Recommended: FieldOption{PgType: "text", Strategy: "direct"}, Reasoning: "looks good"},
	})
	found := false
	for _, w := range schema.Fields[0].Warnings {
		if w == "AI: looks good" {
			found = true
		}
	}
	if !found {
		t.Errorf("AI reasoning not attached as warning: %v", schema.Fields[0].Warnings)
	}
}

func TestApplyAnnotations_UnknownFieldIgnored(t *testing.T) {
	schema := &InferredSchema{
		Fields: []InferredField{
			{Frequency: FieldFrequency{FieldName: "x"}},
		},
	}
	// Annotation for field "y" which doesn't exist — must not panic.
	ApplyAnnotations(schema, []AIAnnotation{
		{FieldName: "y", Recommended: FieldOption{PgType: "text", Strategy: "direct"}},
	})
}

func TestNewAnnotator_ReturnsNoop(t *testing.T) {
	a := NewAnnotator()
	if _, ok := a.(*NoopAnnotator); !ok {
		t.Errorf("NewAnnotator() = %T, want *NoopAnnotator", a)
	}
}
