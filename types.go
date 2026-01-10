package main

import "time"

// ConversationEntry represents a single line in the JSONL file
// Most fields are commented out to save memory - we only need usage info for cost calculation
type ConversationEntry struct {
	// ParentUUID    *string        `json:"parentUuid"`
	// IsSidechain   bool           `json:"isSidechain"`
	// UserType      string         `json:"userType"`
	CWD       string `json:"cwd"`
	GitBranch string `json:"gitBranch"`
	// SessionID     string         `json:"sessionId"`
	// Version       string         `json:"version"`
	// Type          string         `json:"type"`
	Message   Message   `json:"message"`
	UUID      string    `json:"uuid"`
	Timestamp time.Time `json:"timestamp"`
	RequestID *string   `json:"requestId,omitempty"`
	// ToolUseResult *ToolUseResult `json:"toolUseResult,omitempty"`
}

// Message represents the message field - only keeping usage info and model
type Message struct {
	// Role    string         `json:"role"`
	// Content []ContentBlock `json:"content"`
	Model *string `json:"model,omitempty"`
	// ID           *string      `json:"id,omitempty"`
	// Type         *string      `json:"type,omitempty"`
	// StopReason   *string      `json:"stop_reason,omitempty"`
	// StopSequence *string      `json:"stop_sequence,omitempty"`
	Usage *UsageInfo `json:"usage,omitempty"`
}

// UsageInfo represents token usage information
type UsageInfo struct {
	InputTokens              int                `json:"input_tokens"`
	CacheCreationInputTokens int                `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int                `json:"cache_read_input_tokens"`
	OutputTokens             int                `json:"output_tokens"`
	CacheCreation            *CacheCreationInfo `json:"cache_creation,omitempty"`
	ServiceTier              string             `json:"service_tier"`
}

// CacheCreationInfo contains ephemeral cache information
type CacheCreationInfo struct {
	Ephemeral5mInputTokens int `json:"ephemeral_5m_input_tokens"`
	Ephemeral1hInputTokens int `json:"ephemeral_1h_input_tokens"`
}

// OpenCode data types

// OpenCodeMessage represents a message from opencode storage
type OpenCodeMessage struct {
	ID         string          `json:"id"`
	SessionID  string          `json:"sessionID"`
	Role       string          `json:"role"`
	Time       OpenCodeTime    `json:"time"`
	ModelID    string          `json:"modelID"`
	ProviderID string          `json:"providerID"`
	Path       OpenCodePath    `json:"path"`
	Cost       float64         `json:"cost"`
	Tokens     OpenCodeTokens  `json:"tokens"`
}

// OpenCodeTime contains timestamp information (Unix milliseconds)
type OpenCodeTime struct {
	Created   int64 `json:"created"`
	Completed int64 `json:"completed"`
}

// OpenCodePath contains working directory information
type OpenCodePath struct {
	Cwd  string `json:"cwd"`
	Root string `json:"root"`
}

// OpenCodeTokens contains token usage information
type OpenCodeTokens struct {
	Input     int            `json:"input"`
	Output    int            `json:"output"`
	Reasoning int            `json:"reasoning"`
	Cache     OpenCodeCache  `json:"cache"`
}

// OpenCodeCache contains cache token counts
type OpenCodeCache struct {
	Read  int `json:"read"`
	Write int `json:"write"`
}
