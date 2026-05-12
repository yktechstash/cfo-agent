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
		return newHuggingFaceProvider(cfg)
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

type huggingFaceProvider struct {
	baseURL string
	token   string
	model   string
	client  *http.Client
}

func newHuggingFaceProvider(cfg *config.Config) Provider {
	baseURL := strings.TrimSpace(cfg.HuggingFaceBaseURL)
	if baseURL == "" {
		baseURL = "https://router.huggingface.co/v1"
	}
	model := strings.TrimSpace(cfg.HuggingFaceModel)
	if model == "" {
		model = "HuggingFaceTB/SmolLM2-1.7B-Instruct:hf-inference"
	}
	return &huggingFaceProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   strings.TrimSpace(cfg.HuggingFaceToken),
		model:   model,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
}

type hfChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type hfChatCompletionRequest struct {
	Model       string          `json:"model"`
	Messages    []hfChatMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
}

type hfChatCompletionResponse struct {
	Choices []struct {
		Message hfChatMessage `json:"message"`
	} `json:"choices"`
}

func (p *huggingFaceProvider) Complete(ctx context.Context, prompt string) (string, error) {
	if p.token == "" {
		return "", fmt.Errorf("missing HF_TOKEN")
	}

	reqBody, err := json.Marshal(hfChatCompletionRequest{
		Model: p.model,
		Messages: []hfChatMessage{
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
	})
	if err != nil {
		return "", fmt.Errorf("hf request encode: %w", err)
	}

	endpoint := p.baseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("hf request create: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("hf request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", fmt.Errorf("hf read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("hf error %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed hfChatCompletionResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("hf decode response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", errors.New("hf returned no choices")
	}
	content := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if content == "" {
		return "", errors.New("hf returned empty content")
	}
	return content, nil
}
