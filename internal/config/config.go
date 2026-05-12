package config

import "os"

type Config struct {
	PostgresDSN        string
	AnthropicAPIKey    string
	TemporalAddress    string
	ServerPort         string
	AppMode            string
	HuggingFaceToken   string
	HuggingFaceModel   string
	HuggingFaceBaseURL string
}

func Load() *Config {
	return &Config{
		PostgresDSN:        getenv("POSTGRES_DSN", "postgres://reap:reap@localhost:5432/cfo_agent?sslmode=disable"),
		AnthropicAPIKey:    getenv("ANTHROPIC_API_KEY", ""),
		TemporalAddress:    getenv("TEMPORAL_ADDRESS", "localhost:7233"),
		ServerPort:         getenv("SERVER_PORT", ":8080"),
		AppMode:            getenv("APP_MODE", "prod"),
		HuggingFaceToken:   getenv("HF_TOKEN", ""),
		HuggingFaceModel:   getenv("HF_LLM_MODEL", "HuggingFaceTB/SmolLM2-1.7B-Instruct:hf-inference"),
		HuggingFaceBaseURL: getenv("HF_BASE_URL", "https://router.huggingface.co/v1"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
