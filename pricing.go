package main

import "strings"

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
