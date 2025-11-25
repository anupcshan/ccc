package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/go-json-experiment/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
)

// CostRecord represents a record to accumulate
type CostRecord struct {
	RequestID        *string
	Cost             float64
	InputTokens      int
	OutputTokens     int
	CacheReadTokens  int
	CacheWriteTokens int
	InputCost        float64
	OutputCost       float64
	CacheReadCost    float64
	CacheWriteCost   float64
	PricingKey       string // Consolidated model name (opus, sonnet, sonnet-longcontext, haiku-3, etc.)
	Timestamp        string
}

// Metrics holds aggregated metrics for a group
type Metrics struct {
	Cost             float64
	InputTokens      int
	OutputTokens     int
	CacheReadTokens  int
	CacheWriteTokens int
	InputCost        float64
	OutputCost       float64
	CacheReadCost    float64
	CacheWriteCost   float64
}

// GroupConfig defines how to group and display data
type GroupConfig struct {
	LabelColumns  []string                             // Table column headers for labels
	BuildGroupKey func(timestamp, model string) string // Creates group key from record
	ParseGroupKey func(key string) []string            // Extracts labels from group key
	Hierarchical  bool                                 // If true, shows subtotals (e.g., date totals in day,model)
}

// formatTokens formats a token count in a human-readable way
func formatTokens(tokens int) string {
	if tokens == 0 {
		return "0"
	}

	switch {
	case tokens >= 1_000_000_000:
		return fmt.Sprintf("%.1fb", float64(tokens)/1_000_000_000.0)
	case tokens >= 1_000_000:
		return fmt.Sprintf("%.1fm", float64(tokens)/1_000_000.0)
	case tokens >= 1_000:
		return fmt.Sprintf("%.1fk", float64(tokens)/1_000.0)
	default:
		return fmt.Sprintf("%d", tokens)
	}
}

// formatTokensWithCost combines tokens and cost in a single string with padding for alignment
func formatTokensWithCost(tokens int, cost float64, tokenWidth, costWidth int) string {
	tokenStr := formatTokens(tokens)
	costStr := fmt.Sprintf("$%.2f", cost)

	// Right-align both token string and cost for proper alignment (with extra space for readability)
	return fmt.Sprintf("%*s  %*s", tokenWidth, tokenStr, costWidth, costStr)
}

// formatTokensWithCostColored combines tokens and cost with ANSI color based on intensity
func formatTokensWithCostColored(tokens int, cost float64, tokenWidth, costWidth int, intensity float64, colorScheme string) string {
	tokenStr := formatTokens(tokens)
	costStr := fmt.Sprintf("$%.2f", cost)

	// Get color based on intensity and scheme
	color := getColorForIntensity(intensity, colorScheme)

	// Format with color
	formatted := fmt.Sprintf("%*s  %*s", tokenWidth, tokenStr, costWidth, costStr)
	return fmt.Sprintf("\033[38;2;%d;%d;%dm%s\033[0m", color[0], color[1], color[2], formatted)
}

// getColorForIntensity returns RGB values based on intensity (0.0-1.0) and color scheme
func getColorForIntensity(intensity float64, scheme string) [3]int {
	// Clamp intensity between 0 and 1
	if intensity < 0 {
		intensity = 0
	}
	if intensity > 1 {
		intensity = 1
	}

	switch scheme {
	case "blue": // Main data cells
		// Very dim (60, 80, 100) → Medium cyan (80, 180, 220) → BRIGHT cyan (0, 255, 255)
		if intensity < 0.5 {
			// Transition from very dim to medium
			t := intensity * 2
			r := int(60 + (20 * t))
			g := int(80 + (100 * t))
			b := int(100 + (120 * t))
			return [3]int{r, g, b}
		} else {
			// Transition from medium to BRIGHT (this is where it pops)
			t := (intensity - 0.5) * 2
			r := int(80 * (1 - t))
			g := int(180 + (75 * t))
			b := int(220 + (35 * t))
			return [3]int{r, g, b}
		}
	case "orange": // Total column
		// Very dim (120, 80, 40) → Medium (200, 140, 60) → BRIGHT orange (255, 200, 0)
		if intensity < 0.5 {
			t := intensity * 2
			r := int(120 + (80 * t))
			g := int(80 + (60 * t))
			b := int(40 + (20 * t))
			return [3]int{r, g, b}
		} else {
			// Medium to BRIGHT (this is where it pops)
			t := (intensity - 0.5) * 2
			r := int(200 + (55 * t))
			g := int(140 + (60 * t))
			b := int(60 * (1 - t))
			return [3]int{r, g, b}
		}
	case "purple": // Total row
		// Very dim (100, 60, 100) → Medium (180, 100, 180) → BRIGHT magenta (255, 100, 255)
		if intensity < 0.5 {
			t := intensity * 2
			r := int(100 + (80 * t))
			g := int(60 + (40 * t))
			b := int(100 + (80 * t))
			return [3]int{r, g, b}
		} else {
			// Medium to BRIGHT (this is where it pops)
			t := (intensity - 0.5) * 2
			r := int(180 + (75 * t))
			g := 100
			b := int(180 + (75 * t))
			return [3]int{r, g, b}
		}
	default:
		return [3]int{255, 255, 255} // White
	}
}

// ColumnWidths stores the maximum widths needed for token and cost alignment
type ColumnWidths struct {
	InputTokenWidth      int
	InputCostWidth       int
	OutputTokenWidth     int
	OutputCostWidth      int
	CacheReadTokenWidth  int
	CacheReadCostWidth   int
	CacheWriteTokenWidth int
	CacheWriteCostWidth  int
	TotalTokenWidth      int
	TotalCostWidth       int
}

// calculateColumnWidths determines the maximum width needed for each column
func calculateColumnWidths(metricsByGroup map[string]Metrics) ColumnWidths {
	widths := ColumnWidths{}

	for _, m := range metricsByGroup {
		// Token widths
		if w := len(formatTokens(m.InputTokens)); w > widths.InputTokenWidth {
			widths.InputTokenWidth = w
		}
		if w := len(formatTokens(m.OutputTokens)); w > widths.OutputTokenWidth {
			widths.OutputTokenWidth = w
		}
		if w := len(formatTokens(m.CacheReadTokens)); w > widths.CacheReadTokenWidth {
			widths.CacheReadTokenWidth = w
		}
		if w := len(formatTokens(m.CacheWriteTokens)); w > widths.CacheWriteTokenWidth {
			widths.CacheWriteTokenWidth = w
		}

		// Total tokens width
		totalTokens := m.InputTokens + m.OutputTokens + m.CacheReadTokens + m.CacheWriteTokens
		if w := len(formatTokens(totalTokens)); w > widths.TotalTokenWidth {
			widths.TotalTokenWidth = w
		}

		// Cost widths (includes $)
		if w := len(fmt.Sprintf("$%.2f", m.InputCost)); w > widths.InputCostWidth {
			widths.InputCostWidth = w
		}
		if w := len(fmt.Sprintf("$%.2f", m.OutputCost)); w > widths.OutputCostWidth {
			widths.OutputCostWidth = w
		}
		if w := len(fmt.Sprintf("$%.2f", m.CacheReadCost)); w > widths.CacheReadCostWidth {
			widths.CacheReadCostWidth = w
		}
		if w := len(fmt.Sprintf("$%.2f", m.CacheWriteCost)); w > widths.CacheWriteCostWidth {
			widths.CacheWriteCostWidth = w
		}
		if w := len(fmt.Sprintf("$%.2f", m.Cost)); w > widths.TotalCostWidth {
			widths.TotalCostWidth = w
		}
	}

	return widths
}

// HeatmapData stores min/max values for calculating color intensities
type HeatmapData struct {
	MinInput      float64
	MaxInput      float64
	MinOutput     float64
	MaxOutput     float64
	MinCacheRead  float64
	MaxCacheRead  float64
	MinCacheWrite float64
	MaxCacheWrite float64
	MinTotal      float64
	MaxTotal      float64
}

// buildMetricsColumns creates the token and cost columns for a table row
func buildMetricsColumns(m Metrics, widths ColumnWidths) []string {
	totalTokens := m.InputTokens + m.OutputTokens + m.CacheReadTokens + m.CacheWriteTokens
	return []string{
		formatTokensWithCost(m.InputTokens, m.InputCost, widths.InputTokenWidth, widths.InputCostWidth),
		formatTokensWithCost(m.OutputTokens, m.OutputCost, widths.OutputTokenWidth, widths.OutputCostWidth),
		formatTokensWithCost(m.CacheReadTokens, m.CacheReadCost, widths.CacheReadTokenWidth, widths.CacheReadCostWidth),
		formatTokensWithCost(m.CacheWriteTokens, m.CacheWriteCost, widths.CacheWriteTokenWidth, widths.CacheWriteCostWidth),
		formatTokensWithCost(totalTokens, m.Cost, widths.TotalTokenWidth, widths.TotalCostWidth),
	}
}

// buildMetricsColumnsColored creates colored token and cost columns based on heatmap
func buildMetricsColumnsColored(m Metrics, widths ColumnWidths, heatmap HeatmapData, colorScheme string) []string {
	totalTokens := m.InputTokens + m.OutputTokens + m.CacheReadTokens + m.CacheWriteTokens

	// Calculate intensities (0.0 to 1.0)
	inputIntensity := calculateIntensity(m.InputCost, heatmap.MinInput, heatmap.MaxInput)
	outputIntensity := calculateIntensity(m.OutputCost, heatmap.MinOutput, heatmap.MaxOutput)
	cacheReadIntensity := calculateIntensity(m.CacheReadCost, heatmap.MinCacheRead, heatmap.MaxCacheRead)
	cacheWriteIntensity := calculateIntensity(m.CacheWriteCost, heatmap.MinCacheWrite, heatmap.MaxCacheWrite)
	totalIntensity := calculateIntensity(m.Cost, heatmap.MinTotal, heatmap.MaxTotal)

	return []string{
		formatTokensWithCostColored(m.InputTokens, m.InputCost, widths.InputTokenWidth, widths.InputCostWidth, inputIntensity, colorScheme),
		formatTokensWithCostColored(m.OutputTokens, m.OutputCost, widths.OutputTokenWidth, widths.OutputCostWidth, outputIntensity, colorScheme),
		formatTokensWithCostColored(m.CacheReadTokens, m.CacheReadCost, widths.CacheReadTokenWidth, widths.CacheReadCostWidth, cacheReadIntensity, colorScheme),
		formatTokensWithCostColored(m.CacheWriteTokens, m.CacheWriteCost, widths.CacheWriteTokenWidth, widths.CacheWriteCostWidth, cacheWriteIntensity, colorScheme),
		formatTokensWithCostColored(totalTokens, m.Cost, widths.TotalTokenWidth, widths.TotalCostWidth, totalIntensity, colorScheme),
	}
}

// buildMetricsColumnsWithMixedHeatmap uses blue heatmap for first 4 columns, orange for Total column
func buildMetricsColumnsWithMixedHeatmap(m Metrics, widths ColumnWidths, mainHeatmap HeatmapData, totalColumnHeatmap HeatmapData) []string {
	totalTokens := m.InputTokens + m.OutputTokens + m.CacheReadTokens + m.CacheWriteTokens

	// Calculate intensities using main heatmap (blue) for first 4 columns
	inputIntensity := calculateIntensity(m.InputCost, mainHeatmap.MinInput, mainHeatmap.MaxInput)
	outputIntensity := calculateIntensity(m.OutputCost, mainHeatmap.MinOutput, mainHeatmap.MaxOutput)
	cacheReadIntensity := calculateIntensity(m.CacheReadCost, mainHeatmap.MinCacheRead, mainHeatmap.MaxCacheRead)
	cacheWriteIntensity := calculateIntensity(m.CacheWriteCost, mainHeatmap.MinCacheWrite, mainHeatmap.MaxCacheWrite)

	// Calculate intensity using total column heatmap (orange) for Total column
	totalIntensity := calculateIntensity(m.Cost, totalColumnHeatmap.MinTotal, totalColumnHeatmap.MaxTotal)

	return []string{
		formatTokensWithCostColored(m.InputTokens, m.InputCost, widths.InputTokenWidth, widths.InputCostWidth, inputIntensity, "blue"),
		formatTokensWithCostColored(m.OutputTokens, m.OutputCost, widths.OutputTokenWidth, widths.OutputCostWidth, outputIntensity, "blue"),
		formatTokensWithCostColored(m.CacheReadTokens, m.CacheReadCost, widths.CacheReadTokenWidth, widths.CacheReadCostWidth, cacheReadIntensity, "blue"),
		formatTokensWithCostColored(m.CacheWriteTokens, m.CacheWriteCost, widths.CacheWriteTokenWidth, widths.CacheWriteCostWidth, cacheWriteIntensity, "blue"),
		formatTokensWithCostColored(totalTokens, m.Cost, widths.TotalTokenWidth, widths.TotalCostWidth, totalIntensity, "orange"),
	}
}

// calculateIntensity returns a value between 0.0 and 1.0 based on position between min and max
func calculateIntensity(value, min, max float64) float64 {
	if max == min {
		return 0.0
	}
	intensity := (value - min) / (max - min)
	if intensity < 0 {
		return 0.0
	}
	if intensity > 1 {
		return 1.0
	}
	return intensity
}

// calculateHeatmapData computes min/max values for each column across all metrics
func calculateHeatmapData(metrics []Metrics) HeatmapData {
	if len(metrics) == 0 {
		return HeatmapData{}
	}

	heatmap := HeatmapData{
		MinInput:      metrics[0].InputCost,
		MaxInput:      metrics[0].InputCost,
		MinOutput:     metrics[0].OutputCost,
		MaxOutput:     metrics[0].OutputCost,
		MinCacheRead:  metrics[0].CacheReadCost,
		MaxCacheRead:  metrics[0].CacheReadCost,
		MinCacheWrite: metrics[0].CacheWriteCost,
		MaxCacheWrite: metrics[0].CacheWriteCost,
		MinTotal:      metrics[0].Cost,
		MaxTotal:      metrics[0].Cost,
	}

	for _, m := range metrics {
		// Input
		if m.InputCost < heatmap.MinInput {
			heatmap.MinInput = m.InputCost
		}
		if m.InputCost > heatmap.MaxInput {
			heatmap.MaxInput = m.InputCost
		}
		// Output
		if m.OutputCost < heatmap.MinOutput {
			heatmap.MinOutput = m.OutputCost
		}
		if m.OutputCost > heatmap.MaxOutput {
			heatmap.MaxOutput = m.OutputCost
		}
		// Cache Read
		if m.CacheReadCost < heatmap.MinCacheRead {
			heatmap.MinCacheRead = m.CacheReadCost
		}
		if m.CacheReadCost > heatmap.MaxCacheRead {
			heatmap.MaxCacheRead = m.CacheReadCost
		}
		// Cache Write
		if m.CacheWriteCost < heatmap.MinCacheWrite {
			heatmap.MinCacheWrite = m.CacheWriteCost
		}
		if m.CacheWriteCost > heatmap.MaxCacheWrite {
			heatmap.MaxCacheWrite = m.CacheWriteCost
		}
		// Total
		if m.Cost < heatmap.MinTotal {
			heatmap.MinTotal = m.Cost
		}
		if m.Cost > heatmap.MaxTotal {
			heatmap.MaxTotal = m.Cost
		}
	}

	return heatmap
}

// getGroupConfig returns the GroupConfig for a given groupBy mode
func getGroupConfig(groupBy string) GroupConfig {
	configs := map[string]GroupConfig{
		"day": {
			LabelColumns: []string{"Date"},
			BuildGroupKey: func(timestamp, model string) string {
				return timestamp
			},
			ParseGroupKey: func(key string) []string {
				return []string{key}
			},
			Hierarchical: false,
		},
		"model": {
			LabelColumns: []string{"Model"},
			BuildGroupKey: func(timestamp, model string) string {
				return model
			},
			ParseGroupKey: func(key string) []string {
				return []string{key}
			},
			Hierarchical: false,
		},
		"day,model": {
			LabelColumns: []string{"Date", "Model"},
			BuildGroupKey: func(timestamp, model string) string {
				return timestamp + "|" + model
			},
			ParseGroupKey: func(key string) []string {
				return strings.Split(key, "|")
			},
			Hierarchical: true,
		},
	}

	if cfg, ok := configs[groupBy]; ok {
		return cfg
	}
	// Default to "day"
	return configs["day"]
}

// sortKeys sorts keys according to grouping strategy
func sortKeys(keys []string, cfg GroupConfig) {
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if cfg.Hierarchical {
				// For hierarchical (day,model), sort by all parts
				partsI := cfg.ParseGroupKey(keys[i])
				partsJ := cfg.ParseGroupKey(keys[j])

				// Compare each part in order
				shouldSwap := false
				for k := 0; k < len(partsI) && k < len(partsJ); k++ {
					if partsI[k] != partsJ[k] {
						shouldSwap = partsI[k] > partsJ[k]
						break
					}
				}
				if shouldSwap {
					keys[i], keys[j] = keys[j], keys[i]
				}
			} else {
				// Simple string comparison for flat groupings
				if keys[i] > keys[j] {
					keys[i], keys[j] = keys[j], keys[i]
				}
			}
		}
	}
}

// renderTable renders the table with metrics
func renderTable(cfg GroupConfig, keys []string, metricsByGroup map[string]Metrics) {
	// Create table
	table := tablewriter.NewTable(os.Stdout,
		tablewriter.WithRenderer(renderer.NewBlueprint(tw.Rendition{
			Settings: tw.Settings{Separators: tw.Separators{BetweenRows: tw.On}},
		})))

	// Build headers (combined columns)
	headers := append(cfg.LabelColumns,
		"Input",
		"Output",
		"Cache Read",
		"Cache Write",
		"Total")
	table.Header(headers)

	// Configure alignment (labels left, metrics right)
	alignments := make([]tw.Align, len(headers))
	for i := range alignments {
		if i < len(cfg.LabelColumns) {
			alignments[i] = tw.AlignLeft
		} else {
			alignments[i] = tw.AlignRight
		}
	}
	table.Configure(func(c *tablewriter.Config) {
		c.Row.Alignment.PerColumn = alignments
		c.Row.Formatting = tw.CellFormatting{MergeMode: tw.MergeHierarchical}
	})

	// Accumulate totals
	totalMetrics := Metrics{}
	for _, key := range keys {
		m := metricsByGroup[key]
		totalMetrics.Cost += m.Cost
		totalMetrics.InputTokens += m.InputTokens
		totalMetrics.OutputTokens += m.OutputTokens
		totalMetrics.CacheReadTokens += m.CacheReadTokens
		totalMetrics.CacheWriteTokens += m.CacheWriteTokens
		totalMetrics.InputCost += m.InputCost
		totalMetrics.OutputCost += m.OutputCost
		totalMetrics.CacheReadCost += m.CacheReadCost
		totalMetrics.CacheWriteCost += m.CacheWriteCost
	}

	// Calculate column widths for alignment (include total metrics for proper footer alignment)
	allMetrics := make(map[string]Metrics, len(metricsByGroup)+1)
	for k, v := range metricsByGroup {
		allMetrics[k] = v
	}
	allMetrics["__total__"] = totalMetrics
	widths := calculateColumnWidths(allMetrics)

	// Calculate heatmaps for three zones:
	// 1. Main data cells (blue)
	var mainMetrics []Metrics
	for _, key := range keys {
		mainMetrics = append(mainMetrics, metricsByGroup[key])
	}
	mainHeatmap := calculateHeatmapData(mainMetrics)

	// 2. Total column - row totals across all rows (orange)
	var totalColumnMetrics []Metrics
	if cfg.Hierarchical {
		// For hierarchical, include subtotals
		groupsByFirst := make(map[string][]string)
		for _, key := range keys {
			labels := cfg.ParseGroupKey(key)
			firstLabel := labels[0]
			groupsByFirst[firstLabel] = append(groupsByFirst[firstLabel], key)
		}
		for _, groupKeys := range groupsByFirst {
			subtotal := Metrics{}
			for _, key := range groupKeys {
				m := metricsByGroup[key]
				subtotal.Cost += m.Cost
				subtotal.InputTokens += m.InputTokens
				subtotal.OutputTokens += m.OutputTokens
				subtotal.CacheReadTokens += m.CacheReadTokens
				subtotal.CacheWriteTokens += m.CacheWriteTokens
				subtotal.InputCost += m.InputCost
				subtotal.OutputCost += m.OutputCost
				subtotal.CacheReadCost += m.CacheReadCost
				subtotal.CacheWriteCost += m.CacheWriteCost
			}
			totalColumnMetrics = append(totalColumnMetrics, subtotal)
		}
	} else {
		for _, key := range keys {
			totalColumnMetrics = append(totalColumnMetrics, metricsByGroup[key])
		}
	}
	totalColumnHeatmap := calculateHeatmapData(totalColumnMetrics)

	// 3. Total row - create heatmap based on the total row's column values
	// This shows which metric type (Input/Output/CacheRead/CacheWrite) is relatively highest
	totalRowHeatmap := HeatmapData{
		MinInput:      totalMetrics.InputCost,
		MaxInput:      totalMetrics.InputCost,
		MinOutput:     totalMetrics.OutputCost,
		MaxOutput:     totalMetrics.OutputCost,
		MinCacheRead:  totalMetrics.CacheReadCost,
		MaxCacheRead:  totalMetrics.CacheReadCost,
		MinCacheWrite: totalMetrics.CacheWriteCost,
		MaxCacheWrite: totalMetrics.CacheWriteCost,
		MinTotal:      totalMetrics.Cost,
		MaxTotal:      totalMetrics.Cost,
	}

	// Find min/max across all cost types in the total row for relative coloring
	allCosts := []float64{
		totalMetrics.InputCost,
		totalMetrics.OutputCost,
		totalMetrics.CacheReadCost,
		totalMetrics.CacheWriteCost,
	}
	minCost := allCosts[0]
	maxCost := allCosts[0]
	for _, cost := range allCosts {
		if cost < minCost {
			minCost = cost
		}
		if cost > maxCost {
			maxCost = cost
		}
	}
	// Apply same min/max to all columns so they're colored relative to each other
	totalRowHeatmap.MinInput = minCost
	totalRowHeatmap.MaxInput = maxCost
	totalRowHeatmap.MinOutput = minCost
	totalRowHeatmap.MaxOutput = maxCost
	totalRowHeatmap.MinCacheRead = minCost
	totalRowHeatmap.MaxCacheRead = maxCost
	totalRowHeatmap.MinCacheWrite = minCost
	totalRowHeatmap.MaxCacheWrite = maxCost
	// Total column uses the total cost value
	totalRowHeatmap.MinTotal = minCost
	totalRowHeatmap.MaxTotal = maxCost

	if cfg.Hierarchical {
		// Hierarchical rendering (e.g., day,model with date subtotals)
		renderHierarchical(table, cfg, keys, metricsByGroup, totalMetrics, widths, mainHeatmap, totalColumnHeatmap, totalRowHeatmap)
	} else {
		// Flat rendering
		for _, key := range keys {
			labels := cfg.ParseGroupKey(key)
			// Main data cells use blue heatmap, but Total column uses orange
			metricsColumns := buildMetricsColumnsWithMixedHeatmap(metricsByGroup[key], widths, mainHeatmap, totalColumnHeatmap)
			table.Append(append(labels, metricsColumns...))
		}

		// Footer with total (uses purple heatmap)
		footerLabels := make([]string, len(cfg.LabelColumns))
		for i := range footerLabels {
			if i == len(footerLabels)-1 {
				footerLabels[i] = "Total"
			} else {
				footerLabels[i] = ""
			}
		}
		table.Footer(append(footerLabels, buildMetricsColumnsColored(totalMetrics, widths, totalRowHeatmap, "purple")...))
	}

	table.Render()
}

// Pool for scanner buffers to avoid repeated 10MB allocations
var scannerBufferPool = sync.Pool{
	New: func() interface{} {
		const maxCapacity = 10 * 1024 * 1024
		buf := make([]byte, maxCapacity)
		return &buf
	},
}

// SummaryData holds data for template rendering
type SummaryData struct {
	TotalCost        float64
	InputTokens      int
	OutputTokens     int
	CacheReadTokens  int
	CacheWriteTokens int
	TotalTokens      int
	InputCost        float64
	OutputCost       float64
	CacheReadCost    float64
	CacheWriteCost   float64
	// Time-based breakdowns
	Today     Metrics
	ThisWeek  Metrics
	ThisMonth Metrics
	// Pre-formatted strings for aligned output
	TodayCost      string
	ThisWeekCost   string
	ThisMonthCost  string
	TodayTokens    string
	ThisWeekTokens string
	ThisMonthTokens string
}

// Named templates for common summary formats
var namedTemplates = map[string]string{
	"totalcost":   "${{printf \"%.2f\" .TotalCost}}",
	"totaltokens": "{{formatTokens .TotalTokens}}",
	"costsummary": `Today:      ${{.TodayCost}} ({{.TodayTokens}} tokens)
This Week:  ${{.ThisWeekCost}} ({{.ThisWeekTokens}} tokens)
This Month: ${{.ThisMonthCost}} ({{.ThisMonthTokens}} tokens)`,
}

// renderSummary outputs a summary using the provided template format
func renderSummary(metricsByGroup map[string]Metrics, formatStr string, allRecords []CostRecord) error {
	// Check if formatStr is a named template
	if namedTemplate, ok := namedTemplates[formatStr]; ok {
		formatStr = namedTemplate
	}

	// Calculate totals
	totalMetrics := Metrics{}
	for _, m := range metricsByGroup {
		totalMetrics.Cost += m.Cost
		totalMetrics.InputTokens += m.InputTokens
		totalMetrics.OutputTokens += m.OutputTokens
		totalMetrics.CacheReadTokens += m.CacheReadTokens
		totalMetrics.CacheWriteTokens += m.CacheWriteTokens
		totalMetrics.InputCost += m.InputCost
		totalMetrics.OutputCost += m.OutputCost
		totalMetrics.CacheReadCost += m.CacheReadCost
		totalMetrics.CacheWriteCost += m.CacheWriteCost
	}

	// Calculate time-based breakdowns using normalized dates (midnight)
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	weekStart := today.AddDate(0, 0, -int(today.Weekday()))
	monthStart := today.AddDate(0, 0, 1-today.Day())

	todayMetrics := Metrics{}
	weekMetrics := Metrics{}
	monthMetrics := Metrics{}

	for _, record := range allRecords {
		recordDate, err := time.ParseInLocation("2006-01-02", record.Timestamp, now.Location())
		if err != nil {
			continue
		}

		if !recordDate.Before(today) {
			todayMetrics.Cost += record.Cost
			todayMetrics.InputTokens += record.InputTokens
			todayMetrics.OutputTokens += record.OutputTokens
			todayMetrics.CacheReadTokens += record.CacheReadTokens
			todayMetrics.CacheWriteTokens += record.CacheWriteTokens
			todayMetrics.InputCost += record.InputCost
			todayMetrics.OutputCost += record.OutputCost
			todayMetrics.CacheReadCost += record.CacheReadCost
			todayMetrics.CacheWriteCost += record.CacheWriteCost
		}

		if !recordDate.Before(weekStart) {
			weekMetrics.Cost += record.Cost
			weekMetrics.InputTokens += record.InputTokens
			weekMetrics.OutputTokens += record.OutputTokens
			weekMetrics.CacheReadTokens += record.CacheReadTokens
			weekMetrics.CacheWriteTokens += record.CacheWriteTokens
			weekMetrics.InputCost += record.InputCost
			weekMetrics.OutputCost += record.OutputCost
			weekMetrics.CacheReadCost += record.CacheReadCost
			weekMetrics.CacheWriteCost += record.CacheWriteCost
		}

		if !recordDate.Before(monthStart) {
			monthMetrics.Cost += record.Cost
			monthMetrics.InputTokens += record.InputTokens
			monthMetrics.OutputTokens += record.OutputTokens
			monthMetrics.CacheReadTokens += record.CacheReadTokens
			monthMetrics.CacheWriteTokens += record.CacheWriteTokens
			monthMetrics.InputCost += record.InputCost
			monthMetrics.OutputCost += record.OutputCost
			monthMetrics.CacheReadCost += record.CacheReadCost
			monthMetrics.CacheWriteCost += record.CacheWriteCost
		}
	}

	// Calculate total tokens for each period
	todayTotalTokens := todayMetrics.InputTokens + todayMetrics.OutputTokens + todayMetrics.CacheReadTokens + todayMetrics.CacheWriteTokens
	weekTotalTokens := weekMetrics.InputTokens + weekMetrics.OutputTokens + weekMetrics.CacheReadTokens + weekMetrics.CacheWriteTokens
	monthTotalTokens := monthMetrics.InputTokens + monthMetrics.OutputTokens + monthMetrics.CacheReadTokens + monthMetrics.CacheWriteTokens

	// Calculate max widths for alignment
	costs := []float64{todayMetrics.Cost, weekMetrics.Cost, monthMetrics.Cost}
	maxCostWidth := 0
	for _, c := range costs {
		if w := len(fmt.Sprintf("%.2f", c)); w > maxCostWidth {
			maxCostWidth = w
		}
	}

	tokens := []int{todayTotalTokens, weekTotalTokens, monthTotalTokens}
	maxTokenWidth := 0
	for _, t := range tokens {
		if w := len(formatTokens(t)); w > maxTokenWidth {
			maxTokenWidth = w
		}
	}

	// Create template data
	data := SummaryData{
		TotalCost:        totalMetrics.Cost,
		InputTokens:      totalMetrics.InputTokens,
		OutputTokens:     totalMetrics.OutputTokens,
		CacheReadTokens:  totalMetrics.CacheReadTokens,
		CacheWriteTokens: totalMetrics.CacheWriteTokens,
		TotalTokens:      totalMetrics.InputTokens + totalMetrics.OutputTokens + totalMetrics.CacheReadTokens + totalMetrics.CacheWriteTokens,
		InputCost:        totalMetrics.InputCost,
		OutputCost:       totalMetrics.OutputCost,
		CacheReadCost:    totalMetrics.CacheReadCost,
		CacheWriteCost:   totalMetrics.CacheWriteCost,
		Today:            todayMetrics,
		ThisWeek:         weekMetrics,
		ThisMonth:        monthMetrics,
		// Pre-formatted aligned strings
		TodayCost:       fmt.Sprintf("%*s", maxCostWidth, fmt.Sprintf("%.2f", todayMetrics.Cost)),
		ThisWeekCost:    fmt.Sprintf("%*s", maxCostWidth, fmt.Sprintf("%.2f", weekMetrics.Cost)),
		ThisMonthCost:   fmt.Sprintf("%*s", maxCostWidth, fmt.Sprintf("%.2f", monthMetrics.Cost)),
		TodayTokens:     fmt.Sprintf("%*s", maxTokenWidth, formatTokens(todayTotalTokens)),
		ThisWeekTokens:  fmt.Sprintf("%*s", maxTokenWidth, formatTokens(weekTotalTokens)),
		ThisMonthTokens: fmt.Sprintf("%*s", maxTokenWidth, formatTokens(monthTotalTokens)),
	}

	// Parse and execute template
	tmpl, err := template.New("summary").Funcs(template.FuncMap{
		"formatTokens": formatTokens,
		"printf":       fmt.Sprintf,
		"add": func(a, b int) int {
			return a + b
		},
	}).Parse(formatStr)
	if err != nil {
		return fmt.Errorf("failed to parse summary format template: %w", err)
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("failed to execute summary template: %w", err)
	}
	fmt.Println() // Add newline after output

	return nil
}

// renderHierarchical renders hierarchical groupings with subtotals
func renderHierarchical(table *tablewriter.Table, cfg GroupConfig, keys []string, metricsByGroup map[string]Metrics, totalMetrics Metrics, widths ColumnWidths, mainHeatmap HeatmapData, totalColumnHeatmap HeatmapData, totalRowHeatmap HeatmapData) {
	// Group by first label (e.g., date in day,model)
	groupsByFirst := make(map[string][]string)
	for _, key := range keys {
		labels := cfg.ParseGroupKey(key)
		firstLabel := labels[0]
		groupsByFirst[firstLabel] = append(groupsByFirst[firstLabel], key)
	}

	// Get sorted first-level keys
	var firstLevelKeys []string
	for k := range groupsByFirst {
		firstLevelKeys = append(firstLevelKeys, k)
	}
	// Sort first level keys
	for i := 0; i < len(firstLevelKeys); i++ {
		for j := i + 1; j < len(firstLevelKeys); j++ {
			if firstLevelKeys[i] > firstLevelKeys[j] {
				firstLevelKeys[i], firstLevelKeys[j] = firstLevelKeys[j], firstLevelKeys[i]
			}
		}
	}

	// Render each first-level group
	for _, firstKey := range firstLevelKeys {
		groupKeys := groupsByFirst[firstKey]

		// Calculate subtotal
		subtotal := Metrics{}
		for _, key := range groupKeys {
			m := metricsByGroup[key]
			subtotal.Cost += m.Cost
			subtotal.InputTokens += m.InputTokens
			subtotal.OutputTokens += m.OutputTokens
			subtotal.CacheReadTokens += m.CacheReadTokens
			subtotal.CacheWriteTokens += m.CacheWriteTokens
			subtotal.InputCost += m.InputCost
			subtotal.OutputCost += m.OutputCost
			subtotal.CacheReadCost += m.CacheReadCost
			subtotal.CacheWriteCost += m.CacheWriteCost
		}

		// Render subtotal row - uses orange for Total column only (subtotal)
		subtotalLabels := []string{firstKey, "Total"}
		// For subtotal rows, we want: blue for first 4, orange for Total
		subtotalColumns := buildMetricsColumnsWithMixedHeatmap(subtotal, widths, mainHeatmap, totalColumnHeatmap)
		table.Append(append(subtotalLabels, subtotalColumns...))

		// Sort and render detail rows
		sortKeys(groupKeys, cfg)
		for _, key := range groupKeys {
			labels := cfg.ParseGroupKey(key)
			// Main data rows: blue for first 4, orange for Total
			metricsColumns := buildMetricsColumnsWithMixedHeatmap(metricsByGroup[key], widths, mainHeatmap, totalColumnHeatmap)
			table.Append(append(labels, metricsColumns...))
		}
	}

	// Footer with grand total (uses purple heatmap for entire row)
	footerLabels := []string{"", "Total"}
	table.Footer(append(footerLabels, buildMetricsColumnsColored(totalMetrics, widths, totalRowHeatmap, "purple")...))
}

func main() {
	groupBy := flag.String("group-by", "day", "Group results by: day, model, day,model")
	summary := flag.Bool("summary", false, "Display summary output instead of table")
	summaryFormat := flag.String("summary-format", "totalcost", "Summary format: named template or custom Go template")
	cpuProfile := flag.String("cpuprofile", "", "Write CPU profile to file")
	memProfile := flag.String("memprofile", "", "Write memory profile to file")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nNamed Templates:\n")
		fmt.Fprintf(os.Stderr, "  totalcost    - Total cost in dollars (e.g., $239.75)\n")
		fmt.Fprintf(os.Stderr, "  totaltokens  - Total token count (e.g., 366.5m)\n")
		fmt.Fprintf(os.Stderr, "  costsummary  - Today/this week/this month breakdown\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -summary -summary-format=totalcost\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -summary -summary-format=costsummary\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -summary -summary-format='Total: {{formatTokens .TotalTokens}} tokens'\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -group-by=day,model\n", os.Args[0])
	}

	flag.Parse()

	// CPU profiling
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Could not create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Get home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to get home directory: %v", err)
	}

	projectsDir := filepath.Join(homeDir, ".claude", "projects")

	// Collect all JSONL files first
	var jsonlFiles []string
	err = filepath.WalkDir(projectsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(d.Name(), ".jsonl") {
			jsonlFiles = append(jsonlFiles, path)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Error walking directory: %v", err)
	}

	// Get group configuration
	cfg := getGroupConfig(*groupBy)

	// Channel for cost records
	costChan := make(chan CostRecord, 1000)

	// Start accumulator goroutine
	var accWg sync.WaitGroup
	accWg.Add(1)
	metricsByGroup := make(map[string]Metrics)
	var allRecords []CostRecord
	go func() {
		defer accWg.Done()
		// Track the maximum cost record for each requestID
		maxCostByRequestID := make(map[string]CostRecord)

		for record := range costChan {
			if record.RequestID != nil {
				// Track the record with maximum cost for this requestID
				if existing, seen := maxCostByRequestID[*record.RequestID]; !seen {
					maxCostByRequestID[*record.RequestID] = record
				} else if record.Cost > existing.Cost {
					// Found a record with higher cost - update
					maxCostByRequestID[*record.RequestID] = record
				}
			} else {
				// No requestId, accumulate immediately
				groupKey := cfg.BuildGroupKey(record.Timestamp, record.PricingKey)
				m := metricsByGroup[groupKey]
				m.Cost += record.Cost
				m.InputTokens += record.InputTokens
				m.OutputTokens += record.OutputTokens
				m.CacheReadTokens += record.CacheReadTokens
				m.CacheWriteTokens += record.CacheWriteTokens
				m.InputCost += record.InputCost
				m.OutputCost += record.OutputCost
				m.CacheReadCost += record.CacheReadCost
				m.CacheWriteCost += record.CacheWriteCost
				metricsByGroup[groupKey] = m
				allRecords = append(allRecords, record)
			}
		}

		// Now accumulate the max cost for each requestID
		for _, record := range maxCostByRequestID {
			groupKey := cfg.BuildGroupKey(record.Timestamp, record.PricingKey)
			m := metricsByGroup[groupKey]
			m.Cost += record.Cost
			m.InputTokens += record.InputTokens
			m.OutputTokens += record.OutputTokens
			m.CacheReadTokens += record.CacheReadTokens
			m.CacheWriteTokens += record.CacheWriteTokens
			m.InputCost += record.InputCost
			m.OutputCost += record.OutputCost
			m.CacheReadCost += record.CacheReadCost
			m.CacheWriteCost += record.CacheWriteCost
			metricsByGroup[groupKey] = m
			allRecords = append(allRecords, record)
		}
	}()

	// Global channel for lines to parse
	lineChan := make(chan []byte, 1000)

	// Start global worker pool for parsing lines
	var lineWg sync.WaitGroup
	numLineWorkers := runtime.NumCPU()
	for range numLineWorkers {
		lineWg.Go(func() {
			for line := range lineChan {
				var entry ConversationEntry
				if err := json.Unmarshal(line, &entry); err != nil {
					log.Printf("Error decoding line: %v", err)
					continue
				}

				// Calculate cost and get pricing key
				cost, inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens, inputCost, outputCost, cacheReadCost, cacheWriteCost, pricingKey := CalculateCost(&entry.Message)

				// Skip entries with no valid pricing
				if pricingKey == "" {
					continue
				}

				record := CostRecord{
					RequestID:        entry.RequestID,
					Cost:             cost,
					InputTokens:      inputTokens,
					OutputTokens:     outputTokens,
					CacheReadTokens:  cacheReadTokens,
					CacheWriteTokens: cacheWriteTokens,
					InputCost:        inputCost,
					OutputCost:       outputCost,
					CacheReadCost:    cacheReadCost,
					CacheWriteCost:   cacheWriteCost,
					PricingKey:       pricingKey,
					Timestamp:        entry.Timestamp.Local().Format("2006-01-02"),
				}
				costChan <- record
			}
		})
	}

	// Process files in parallel
	var fileWg sync.WaitGroup
	fileChan := make(chan string, len(jsonlFiles))

	// Start worker pool for file reading
	numFileWorkers := min(runtime.NumCPU(), 4)
	for range numFileWorkers {
		fileWg.Go(func() {
			buf := make([]byte, 2*1024*1024)
			for path := range fileChan {
				if err := processJSONLFile(path, lineChan, buf); err != nil {
					log.Printf("Error processing file %s: %v", path, err)
				}
			}
		})
	}

	// Send files to workers
	for _, path := range jsonlFiles {
		fileChan <- path
	}
	close(fileChan)

	// Wait for all files to be read
	fileWg.Wait()
	// Close line channel and wait for all parsing to complete
	close(lineChan)
	lineWg.Wait()

	// Close cost channel and wait for accumulator
	close(costChan)
	accWg.Wait()

	// Render output based on flags
	if *summary {
		// Render summary using template
		if err := renderSummary(metricsByGroup, *summaryFormat, allRecords); err != nil {
			log.Fatalf("Error rendering summary: %v", err)
		}
	} else {
		// Collect and sort keys
		var keys []string
		for key := range metricsByGroup {
			keys = append(keys, key)
		}
		sortKeys(keys, cfg)

		// Render table
		renderTable(cfg, keys, metricsByGroup)
	}

	// Memory profiling
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatalf("Could not create memory profile: %v", err)
		}
		defer f.Close()
		runtime.GC() // Get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("Could not write memory profile: %v", err)
		}
	}
}

func processJSONLFile(path string, lineChan chan<- []byte, buffer []byte) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, len(buffer))

	for scanner.Scan() {
		line := scanner.Bytes()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Make a copy of the line since scanner reuses the buffer
		lineCopy := make([]byte, len(line))
		copy(lineCopy, line)
		lineChan <- lineCopy
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	return nil
}
