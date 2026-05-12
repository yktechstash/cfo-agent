package llm

import (
	"context"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/reap/cfo-agent/internal/config"
)

var client anthropic.Client

func Init(cfg *config.Config) {
	client = anthropic.NewClient(
		option.WithAPIKey(cfg.AnthropicAPIKey),
	)
}

// Complete sends a prompt to Claude and returns the raw text response.
// The caller (tag.go) is responsible for parsing and validating the JSON.
//
// Model: claude-sonnet-4-20250514 — best balance of reasoning and cost
// for structured extraction tasks. Opus is overkill here; Haiku lacks
// reasoning depth for multi-signal CoA selection.
func Complete(ctx context.Context, prompt string) (string, error) {
	//if client == nil {
	//	return "", fmt.Errorf("llm client not initialised — call llm.Init() at startup")
	//}

	msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.ModelClaudeSonnet4_5,
		MaxTokens: 256, // enough for the JSON response, never more
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		},
		// System prompt keeps the model focused and JSON-only
		System: []anthropic.TextBlockParam{
			{
				Type: "text",
				Text: "You are a financial transaction tagger. " +
					"You respond ONLY with a valid JSON object. " +
					"Never explain, never use markdown. " +
					"If you are uncertain, reflect that in a lower confidence score — " +
					"do not guess with high confidence.",
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("anthropic api error: %w", err)
	}

	if len(msg.Content) == 0 {
		return "", fmt.Errorf("anthropic returned empty content")
	}

	// Extract the text block
	for _, block := range msg.Content {
		if block.Type == "text" {
			return block.Text, nil
		}
	}
	return "", fmt.Errorf("no text block in anthropic response")
}
