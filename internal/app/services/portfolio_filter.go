package services

import (
	"fmt"
	"sort"
	"strings"
)

type PortfolioFilterType string

const (
	PortfolioFilterGroup      PortfolioFilterType = "group"
	PortfolioFilterReferences PortfolioFilterType = "portfolio_references"
)

type PortfolioFilter struct {
	Type       PortfolioFilterType
	Values     []string
	References []PortfolioReference
	Raw        string
}

type PortfolioReference struct {
	PortfolioID     string `json:"portfolioId,omitempty"`
	PortfolioTicker string `json:"portfolioTicker,omitempty"`
}

func ParsePortfolioFilter(value string) (PortfolioFilter, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return PortfolioFilter{}, fmt.Errorf("portfolio filter cannot be empty")
	}

	prefix, body, hasPrefix := strings.Cut(raw, ":")
	filterType := PortfolioFilterReferences
	if hasPrefix {
		body = strings.TrimSpace(body)
		switch normalizeFilterPrefix(prefix) {
		case "group":
			filterType = PortfolioFilterGroup
		case "portfolio":
			filterType = PortfolioFilterReferences
		default:
			return PortfolioFilter{}, fmt.Errorf("unsupported portfolio filter prefix %q", prefix)
		}
	} else {
		body = raw
	}

	values := splitPortfolioFilterValues(body)
	if len(values) == 0 {
		return PortfolioFilter{}, fmt.Errorf("portfolio filter cannot be empty")
	}
	if filterType == PortfolioFilterGroup && len(values) != 1 {
		return PortfolioFilter{}, fmt.Errorf("portfolio group filter accepts exactly one value")
	}

	return PortfolioFilter{
		Type:   filterType,
		Values: values,
		Raw:    raw,
	}, nil
}

func (f PortfolioFilter) String() string {
	if f.Raw != "" {
		return f.Raw
	}
	return strings.Join(f.Values, ",")
}

func (f PortfolioFilter) Criteria() map[string]any {
	if f.Type == PortfolioFilterGroup {
		return map[string]any{"portfolioGroupTicker": f.Values[0]}
	}
	if len(f.References) > 0 {
		return map[string]any{"portfolioReferences": f.References}
	}
	references := make([]PortfolioReference, 0, len(f.Values))
	for _, value := range f.Values {
		references = append(references, PortfolioReference{PortfolioTicker: value})
	}
	return map[string]any{"portfolioReferences": references}
}

func (f PortfolioFilter) referenceCount() int {
	if len(f.References) > 0 {
		return len(f.References)
	}
	return len(f.Values)
}

func (f PortfolioFilter) split() (PortfolioFilter, PortfolioFilter) {
	middle := f.referenceCount() / 2
	if middle < 1 {
		return f, PortfolioFilter{}
	}
	left, right := f, f
	left.Values = append([]string(nil), f.Values[:middle]...)
	right.Values = append([]string(nil), f.Values[middle:]...)
	if len(f.References) > 0 {
		left.References = append([]PortfolioReference(nil), f.References[:middle]...)
		right.References = append([]PortfolioReference(nil), f.References[middle:]...)
	}
	left.Raw = strings.Join(left.Values, ",")
	right.Raw = strings.Join(right.Values, ",")
	return left, right
}

func (f PortfolioFilter) checkpointKey() string {
	values := make([]string, 0, len(f.Values))
	for _, value := range f.Values {
		if value = strings.ToUpper(strings.TrimSpace(value)); value != "" {
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return string(f.Type) + ":" + strings.Join(values, ",")
}

func normalizeFilterPrefix(prefix string) string {
	normalized := strings.ToLower(strings.TrimSpace(prefix))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case "group", "portfolio_group", "port_group", "portfolio_group_ticker":
		return "group"
	case "portfolio", "portfolios", "portfolio_reference", "portfolio_references", "portfolio_ticker", "portfolio_tickers":
		return "portfolio"
	default:
		return normalized
	}
}

func splitPortfolioFilterValues(value string) []string {
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		key := strings.ToUpper(trimmed)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, trimmed)
	}
	return values
}
