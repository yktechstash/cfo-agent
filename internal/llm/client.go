package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/reap/cfo-agent/internal/config"
)

type Provider interface {
	Complete(ctx context.Context, prompt string) (string, error)
}

var provider Provider

func Init(cfg *config.Config) {
	provider = newProvider(cfg)
}

func Complete(ctx context.Context, prompt string) (string, error) {
	if provider == nil {
		return "", fmt.Errorf("llm provider not initialised")
	}
	return provider.Complete(ctx, prompt)
}

func newProvider(cfg *config.Config) Provider {
	mode := strings.ToLower(strings.TrimSpace(cfg.AppMode))
	if mode == "test" {
		return newLocalOpenAIProvider(cfg)
	}
	return newAnthropicProvider(cfg)
}

type anthropicProvider struct {
	client anthropic.Client
}

func newAnthropicProvider(cfg *config.Config) Provider {
	return &anthropicProvider{
		client: anthropic.NewClient(option.WithAPIKey(cfg.AnthropicAPIKey)),
	}
}

func (p *anthropicProvider) Complete(ctx context.Context, prompt string) (string, error) {
	msg, err := p.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.ModelClaudeSonnet4_5,
		MaxTokens: 256,
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		},
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
	for _, block := range msg.Content {
		if block.Type == "text" {
			return block.Text, nil
		}
	}
	return "", fmt.Errorf("no text block in anthropic response")
}

type localOpenAIProvider struct {
	baseURL string
	model   string
	client  *http.Client
}

func newLocalOpenAIProvider(cfg *config.Config) Provider {
	baseURL := strings.TrimSpace(cfg.LocalLLMBaseURL)
	if baseURL == "" {
		baseURL = "http://llm:8080/v1"
	}
	model := strings.TrimSpace(cfg.LocalLLMModel)
	if model == "" {
		model = "smollm2"
	}
	return &localOpenAIProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		client:  &http.Client{Timeout: 120 * time.Second},
	}
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Temperature float64       `json:"temperature,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

func (p *localOpenAIProvider) Complete(ctx context.Context, prompt string) (string, error) {
	reqBody, err := json.Marshal(chatCompletionRequest{
		Model: p.model,
		Messages: []chatMessage{
			{
				Role: "system",
				Content: "You are a financial transaction tagger. " +
					"You respond ONLY with a valid JSON object. " +
					"Never explain, never use markdown.",
			},
			{Role: "user", Content: prompt},
		},
		MaxTokens:   256,
		Temperature: 0,
		Stream:      false,
	})
	if err != nil {
		return "", fmt.Errorf("local llm request encode: %w", err)
	}

	endpoint := p.baseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("local llm request create: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("local llm request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", fmt.Errorf("local llm read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("local llm error %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed chatCompletionResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("local llm decode response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", errors.New("local llm returned no choices")
	}
	content := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if content == "" {
		return "", errors.New("local llm returned empty content")
	}
	return content, nil
}
