package openrouter

import (
	"testing"

	"github.com/Desarso/godantic/models"
)

func TestRequestsEnforceZeroDataRetentionByDefault(t *testing.T) {
	model := &OpenRouter_Model{Model: "openai/gpt-6-luna"}
	request, err := model.createOpenRouterRequest(model.Model, models.User_Message{Content: models.Content{Parts: []models.User_Part{{Text: "hi"}}}}, nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if request.Provider["zdr"] != true {
		t.Fatalf("provider = %v, want zdr", request.Provider)
	}
}

func TestProviderPreferencesOverrideAndCustomBaseURL(t *testing.T) {
	custom := map[string]any{"only": []string{"azure"}}
	if got := (&OpenRouter_Model{Provider: custom}).providerPreferences(); got["only"] == nil || got["zdr"] != nil {
		t.Fatalf("caller preferences replaced: %v", got)
	}
	if got := (&OpenRouter_Model{BaseURL: "https://example.test/v1"}).providerPreferences(); got != nil {
		t.Fatalf("custom base URL got provider preferences: %v", got)
	}
}
