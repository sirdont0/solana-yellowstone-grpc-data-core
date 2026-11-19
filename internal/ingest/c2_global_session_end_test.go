package ingest

import "testing"

// The legacy/global adapter must retain the same incomplete-bank semantics
// as source-scoped WAL replay, while affecting only actually open banks.
func TestC2GlobalSessionEndDoesNotHideOpenBanks(t *testing.T) {
	c := mustMultiSource(t, multiSourceSpecs("independent-b"), 8)
	connectMultiSource(t, c, "A", 1)
	connectMultiSource(t, c, "B", 1)
	metaMultiSource(t, c, "A", 1, 720, 0, 719, "hash-720")
	metaMultiSource(t, c, "B", 1, 721, 0, 720, "hash-721")
	d, err := c.ObserveRecord(Record{Type: "marker", Kind: "session_end", Detail: "global shutdown"})
	if err != nil {
		t.Fatal(err)
	}
	if !d.Processed || d.Quality != MultiSourceBroken {
		t.Fatalf("session end suppressed uncertainty: %+v", d)
	}
	for _, source := range []string{"A", "B"} {
		h := healthMultiSource(t, c, source)
		if h.Connected || !h.ContinuityBroken || h.PartialBanks != 1 {
			t.Fatalf("open bank for %s was not marked partial: %+v", source, h)
		}
	}
	assertNoExactPromotion(t, c.Snapshot())
}
