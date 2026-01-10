package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ModelPricing represents the pricing for a model in dollars per million tokens
type ModelPricing struct {
	Input        float64 // Base input tokens
	Cache5mWrite float64 // 5m cache writes
	Cache1hWrite float64 // 1h cache writes
	CacheRead    float64 // Cache hits & refreshes
	Output       float64 // Output tokens
}

// Pricing table for Claude model families (per million tokens)
var modelPricing = map[string]ModelPricing{
	"opus-4.5": {
		Input:        5.00,
		Cache5mWrite: 6.25,
		Cache1hWrite: 10.00, // 2× input, following standard pattern
		CacheRead:    0.50,
		Output:       25.00,
	},
	"opus": {
		Input:        15.00,
		Cache5mWrite: 18.75,
		Cache1hWrite: 30.00,
		CacheRead:    1.50,
		Output:       75.00,
	},
	"sonnet": {
		Input:        3.00,
		Cache5mWrite: 3.75,
		Cache1hWrite: 6.00,
		CacheRead:    0.30,
		Output:       15.00,
	},
	"sonnet-longcontext": {
		Input:        6.00,
		Cache5mWrite: 7.50,  // Proportionally scaled
		Cache1hWrite: 12.00, // Proportionally scaled
		CacheRead:    0.60,  // Proportionally scaled
		Output:       22.50,
	},
	"haiku-4.5": {
		Input:        1.00,
		Cache5mWrite: 1.25,
		Cache1hWrite: 2.00,
		CacheRead:    0.10,
		Output:       5.00,
	},
	"haiku-3.5": {
		Input:        0.80,
		Cache5mWrite: 1.00,
		Cache1hWrite: 1.60,
		CacheRead:    0.08,
		Output:       4.00,
	},
	"haiku-3": {
		Input:        0.25,
		Cache5mWrite: 0.30,
		Cache1hWrite: 0.50,
		CacheRead:    0.03,
		Output:       1.25,
	},
}

// isSonnet4 checks if the model is Sonnet 4 or 4.5
func isSonnet4(model string) bool {
	modelLower := strings.ToLower(model)
	if !strings.Contains(modelLower, "sonnet") {
		return false
	}
	// Sonnet 4 or 4.5 (not 3.x)
	return strings.Contains(modelLower, "sonnet-4") || strings.Contains(modelLower, "sonnet_4")
}

// GetModelPricing returns pricing for a model by detecting the family
// Returns (pricing, pricingKey, ok)
func GetModelPricing(model string, usage *UsageInfo) (ModelPricing, string, bool) {
	modelLower := strings.ToLower(model)

	// Check for Opus
	if strings.Contains(modelLower, "opus") {
		// Opus 4.5 has different pricing
		if strings.Contains(modelLower, "4.5") || strings.Contains(modelLower, "4-5") {
			return modelPricing["opus-4.5"], "opus-4.5", true
		}
		return modelPricing["opus"], "opus", true
	}

	// Check for Sonnet (all versions same price)
	if strings.Contains(modelLower, "sonnet") {
		// Check if this is Sonnet 4/4.5 with > 200K input tokens
		if usage != nil && isSonnet4(model) {
			totalInputTokens := usage.InputTokens + usage.CacheCreationInputTokens + usage.CacheReadInputTokens
			if totalInputTokens > 200_000 {
				return modelPricing["sonnet-longcontext"], "sonnet-longcontext", true
			}
		}
		return modelPricing["sonnet"], "sonnet", true
	}

	// Check for Haiku variants
	if strings.Contains(modelLower, "haiku") {
		// Check for specific versions
		if strings.Contains(modelLower, "4.5") || strings.Contains(modelLower, "4-5") {
			return modelPricing["haiku-4.5"], "haiku-4.5", true
		}
		if strings.Contains(modelLower, "3.5") || strings.Contains(modelLower, "3-5") {
			return modelPricing["haiku-3.5"], "haiku-3.5", true
		}
		// Default to Haiku 3 for older versions or unspecified
		return modelPricing["haiku-3"], "haiku-3", true
	}

	return ModelPricing{}, "", false
}

// CalculateCost calculates the cost in dollars for a message
// Returns (cost, inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens, inputCost, outputCost, cacheReadCost, cacheWriteCost, pricingKey).
// pricingKey is empty if no valid pricing found.
func CalculateCost(msg *Message) (float64, int, int, int, int, float64, float64, float64, float64, string) {
	if msg == nil || msg.Usage == nil || msg.Model == nil {
		return 0.0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, ""
	}

	pricing, pricingKey, ok := GetModelPricing(*msg.Model, msg.Usage)
	if !ok {
		return 0.0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, ""
	}

	usage := msg.Usage

	// Base input tokens
	inputCost := float64(usage.InputTokens) / 1_000_000.0 * pricing.Input

	// Cache write tokens (5m and 1h separately)
	cacheWriteTokens := 0
	cacheWriteCost := 0.0
	if usage.CacheCreation != nil {
		cacheWriteTokens = usage.CacheCreation.Ephemeral5mInputTokens + usage.CacheCreation.Ephemeral1hInputTokens
		cacheWriteCost += float64(usage.CacheCreation.Ephemeral5mInputTokens) / 1_000_000.0 * pricing.Cache5mWrite
		cacheWriteCost += float64(usage.CacheCreation.Ephemeral1hInputTokens) / 1_000_000.0 * pricing.Cache1hWrite
	}

	// Cache read tokens
	cacheReadCost := float64(usage.CacheReadInputTokens) / 1_000_000.0 * pricing.CacheRead

	// Output tokens
	outputCost := float64(usage.OutputTokens) / 1_000_000.0 * pricing.Output

	totalCost := inputCost + cacheWriteCost + cacheReadCost + outputCost

	return totalCost, usage.InputTokens, usage.OutputTokens, usage.CacheReadInputTokens, cacheWriteTokens, inputCost, outputCost, cacheReadCost, cacheWriteCost, pricingKey
}

// OpenRouter pricing types and caching

// OpenRouterModel represents a model from the OpenRouter API
type OpenRouterModel struct {
	ID      string              `json:"id"`
	Pricing OpenRouterPricing   `json:"pricing"`
}

// OpenRouterPricing contains per-token pricing from OpenRouter (as strings, converted to float)
type OpenRouterPricing struct {
	Prompt          string `json:"prompt"`           // Cost per input token
	Completion      string `json:"completion"`       // Cost per output token
	InputCacheRead  string `json:"input_cache_read"` // Cost per cache read token
	InputCacheWrite string `json:"input_cache_write"`// Cost per cache write token
}

// OpenRouterResponse is the API response structure
type OpenRouterResponse struct {
	Data []OpenRouterModel `json:"data"`
}

// OpenRouterCache holds the cached pricing data
type OpenRouterCache struct {
	FetchedAt time.Time                  `json:"fetched_at"`
	Models    map[string]OpenRouterModel `json:"models"` // keyed by model ID
}

var (
	openRouterCache     *OpenRouterCache
	openRouterCacheMu   sync.RWMutex
	openRouterCacheFile string
)

const openRouterCacheMaxAge = 24 * time.Hour

func init() {
	// Set up cache file path
	dataDir := os.Getenv("XDG_DATA_HOME")
	if dataDir == "" {
		homeDir, _ := os.UserHomeDir()
		dataDir = filepath.Join(homeDir, ".local", "share")
	}
	openRouterCacheFile = filepath.Join(dataDir, "ccc", "openrouter-pricing.json")
}

// loadOpenRouterCache loads the cached pricing from disk
func loadOpenRouterCache() (*OpenRouterCache, error) {
	openRouterCacheMu.RLock()
	if openRouterCache != nil {
		defer openRouterCacheMu.RUnlock()
		return openRouterCache, nil
	}
	openRouterCacheMu.RUnlock()

	openRouterCacheMu.Lock()
	defer openRouterCacheMu.Unlock()

	// Double-check after acquiring write lock
	if openRouterCache != nil {
		return openRouterCache, nil
	}

	// Try to load from disk
	data, err := os.ReadFile(openRouterCacheFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No cache file yet
		}
		return nil, err
	}

	var cache OpenRouterCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}

	openRouterCache = &cache
	return openRouterCache, nil
}

// saveOpenRouterCache saves the pricing cache to disk
func saveOpenRouterCache(cache *OpenRouterCache) error {
	// Ensure directory exists
	dir := filepath.Dir(openRouterCacheFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(openRouterCacheFile, data, 0644)
}

// fetchOpenRouterPricing fetches pricing from OpenRouter API
func fetchOpenRouterPricing() (*OpenRouterCache, error) {
	resp, err := http.Get("https://openrouter.ai/api/v1/models")
	if err != nil {
		return nil, fmt.Errorf("fetching OpenRouter models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenRouter API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading OpenRouter response: %w", err)
	}

	var apiResp OpenRouterResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("parsing OpenRouter response: %w", err)
	}

	cache := &OpenRouterCache{
		FetchedAt: time.Now(),
		Models:    make(map[string]OpenRouterModel),
	}

	for _, model := range apiResp.Data {
		cache.Models[model.ID] = model
	}

	return cache, nil
}

// ensureOpenRouterPricing ensures we have up-to-date OpenRouter pricing
func ensureOpenRouterPricing() *OpenRouterCache {
	cache, _ := loadOpenRouterCache()

	// Check if cache is fresh enough
	if cache != nil && time.Since(cache.FetchedAt) < openRouterCacheMaxAge {
		return cache
	}

	// Fetch fresh pricing
	newCache, err := fetchOpenRouterPricing()
	if err != nil {
		// If fetch fails but we have old cache, use it
		if cache != nil {
			return cache
		}
		return nil
	}

	// Save to disk and memory
	openRouterCacheMu.Lock()
	openRouterCache = newCache
	openRouterCacheMu.Unlock()

	if err := saveOpenRouterCache(newCache); err != nil {
		// Log but don't fail
		fmt.Fprintf(os.Stderr, "Warning: could not save OpenRouter cache: %v\n", err)
	}

	return newCache
}

// GetOpenRouterPricing looks up pricing for a model from OpenRouter
// Returns (pricing, pricingKey, ok)
func GetOpenRouterPricing(modelID string) (ModelPricing, string, bool) {
	cache := ensureOpenRouterPricing()
	if cache == nil {
		return ModelPricing{}, "", false
	}

	// Try exact match first
	model, ok := cache.Models[modelID]

	// Fallback: scan all models and match by the model name part (after last slash)
	if !ok {
		modelIDLower := strings.ToLower(modelID)
		for id, m := range cache.Models {
			// Extract the model name from "provider/model-name"
			parts := strings.Split(id, "/")
			if len(parts) >= 2 {
				modelName := strings.ToLower(parts[len(parts)-1])
				if modelName == modelIDLower {
					model = m
					ok = true
					break
				}
			}
		}
	}

	if !ok {
		return ModelPricing{}, "", false
	}

	// Convert per-token pricing to per-million-token pricing
	promptCost, _ := strconv.ParseFloat(model.Pricing.Prompt, 64)
	completionCost, _ := strconv.ParseFloat(model.Pricing.Completion, 64)
	cacheReadCost, _ := strconv.ParseFloat(model.Pricing.InputCacheRead, 64)
	cacheWriteCost, _ := strconv.ParseFloat(model.Pricing.InputCacheWrite, 64)

	pricing := ModelPricing{
		Input:        promptCost * 1_000_000,
		Output:       completionCost * 1_000_000,
		CacheRead:    cacheReadCost * 1_000_000,
		Cache5mWrite: cacheWriteCost * 1_000_000, // OpenRouter doesn't distinguish 5m/1h
		Cache1hWrite: cacheWriteCost * 1_000_000,
	}

	return pricing, model.ID, true
}

// CalculateCostWithDynamicPricing calculates cost using hardcoded Claude pricing or OpenRouter fallback
// Returns same values as CalculateCost, plus whether OpenRouter pricing was used
func CalculateCostWithDynamicPricing(model string, inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens int) (float64, float64, float64, float64, float64, string, bool) {
	// First try hardcoded Claude pricing
	pricing, pricingKey, ok := GetModelPricing(model, nil)
	usedOpenRouter := false

	if !ok {
		// Fall back to OpenRouter
		pricing, pricingKey, ok = GetOpenRouterPricing(model)
		if ok {
			usedOpenRouter = true
		}
	}

	if !ok {
		// No pricing found - return zeros but with model name as key
		return 0.0, 0.0, 0.0, 0.0, 0.0, model, false
	}

	inputCost := float64(inputTokens) / 1_000_000.0 * pricing.Input
	outputCost := float64(outputTokens) / 1_000_000.0 * pricing.Output
	cacheReadCost := float64(cacheReadTokens) / 1_000_000.0 * pricing.CacheRead
	cacheWriteCost := float64(cacheWriteTokens) / 1_000_000.0 * pricing.Cache5mWrite // Use 5m rate as default

	totalCost := inputCost + outputCost + cacheReadCost + cacheWriteCost

	return totalCost, inputCost, outputCost, cacheReadCost, cacheWriteCost, pricingKey, usedOpenRouter
}
