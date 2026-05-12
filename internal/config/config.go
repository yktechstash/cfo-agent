package config

import "os"

type Config struct {
	PostgresDSN     string
	AnthropicAPIKey string
	TemporalAddress string
	ServerPort      string
	AppMode         string
	LocalLLMBaseURL string
	LocalLLMModel   string
}

func Load() *Config {
	return &Config{
		PostgresDSN:     getenv("POSTGRES_DSN", "postgres://reap:reap@localhost:5432/cfo_agent?sslmode=disable"),
		AnthropicAPIKey: getenv("ANTHROPIC_API_KEY", ""),
		TemporalAddress: getenv("TEMPORAL_ADDRESS", "localhost:7233"),
		ServerPort:      getenv("SERVER_PORT", ":8080"),
		AppMode:         getenv("APP_MODE", "prod"),
		LocalLLMBaseURL: getenv("LOCAL_LLM_BASE_URL", "http://llm:8080/v1"),
		LocalLLMModel:   getenv("LOCAL_LLM_MODEL", "smollm2"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
