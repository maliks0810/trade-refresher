package services

import "testing"

func TestParsePortfolioFilterGroup(t *testing.T) {
	filter, err := ParsePortfolioFilter("group:TCW_ALL")
	if err != nil {
		t.Fatalf("ParsePortfolioFilter returned error: %v", err)
	}
	if filter.Type != PortfolioFilterGroup || len(filter.Values) != 1 || filter.Values[0] != "TCW_ALL" {
		t.Fatalf("unexpected group filter: %#v", filter)
	}

	criteria := filter.Criteria()
	if criteria["portfolioGroupTicker"] != "TCW_ALL" {
		t.Fatalf("unexpected criteria: %#v", criteria)
	}
}

func TestParsePortfolioFilterReferences(t *testing.T) {
	filter, err := ParsePortfolioFilter("702T, 710T, 3409T, 702T")
	if err != nil {
		t.Fatalf("ParsePortfolioFilter returned error: %v", err)
	}
	if filter.Type != PortfolioFilterReferences {
		t.Fatalf("filter type = %s, want portfolio references", filter.Type)
	}
	if got, want := len(filter.Values), 3; got != want {
		t.Fatalf("values = %d, want %d: %#v", got, want, filter.Values)
	}

	references, ok := filter.Criteria()["portfolioReferences"].([]PortfolioReference)
	if !ok {
		t.Fatalf("portfolioReferences missing from criteria: %#v", filter.Criteria())
	}
	if references[0].PortfolioTicker != "702T" || references[1].PortfolioTicker != "710T" || references[2].PortfolioTicker != "3409T" {
		t.Fatalf("unexpected portfolio references: %#v", references)
	}
}

func TestPortfolioFilterCheckpointKeyIsOrderAndCaseIndependent(t *testing.T) {
	first, err := ParsePortfolioFilter("702T, 710t")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ParsePortfolioFilter("710T,702t")
	if err != nil {
		t.Fatal(err)
	}
	if first.checkpointKey() != second.checkpointKey() {
		t.Fatalf("checkpoint keys differ: %q != %q", first.checkpointKey(), second.checkpointKey())
	}
}

func TestParsePortfolioFilterRejectsInvalidGroupList(t *testing.T) {
	if _, err := ParsePortfolioFilter("group:TCW_ALL,OTHER"); err == nil {
		t.Fatal("expected invalid group list to fail")
	}
}
