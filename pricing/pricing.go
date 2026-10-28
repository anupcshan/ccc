// Package pricing reports the model prices used by ccc.
package pricing

import (
	"time"

	internalpricing "github.com/anupcshan/ccc/internal/pricing"
)

// ModelPricing holds a model's prices in dollars per million tokens.
type ModelPricing = internalpricing.ModelPricing

// GetModelPricing returns the pricing for model at the given timestamp.
func GetModelPricing(model string, timestamp time.Time) *ModelPricing {
	return internalpricing.GetModelPricing(model, nil, timestamp)
}
