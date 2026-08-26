package ai

import (
	"context"

	"gitea.homelab.local/nextdevops/XferDB/analyzer"
)

// Suggester is implemented by AI providers that can augment schema analysis.
type Suggester interface {
	Available() bool
	Suggest(ctx context.Context, analysis *analyzer.Analysis) ([]string, error)
}

// NoopSuggester is the default implementation used when no AI provider is configured.
type NoopSuggester struct{}

func (NoopSuggester) Available() bool { return false }

func (NoopSuggester) Suggest(_ context.Context, _ *analyzer.Analysis) ([]string, error) {
	return nil, nil
}
