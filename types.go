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
