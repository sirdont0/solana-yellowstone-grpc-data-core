package ingest

import "testing"

func TestRawPayloadFitsWALFrameBoundary(t *testing.T) {
	max := 0
	for raw := 0; raw <= maxFrame; raw += 4096 {
		if rawPayloadFitsWAL(raw) {
			max = raw
		}
	}
	for raw := max; raw <= max+4096; raw++ {
		if rawPayloadFitsWAL(raw) {
			max = raw
		} else {
			break
		}
	}
	if !rawPayloadFitsWAL(max) || rawPayloadFitsWAL(max+1) {
		t.Fatalf("payload boundary incorrect max=%d", max)
	}
	if !rawPayloadFitsWAL(95 << 20) {
		t.Fatal("95 MiB protobuf payload must fit with the 1 MiB envelope reserve")
	}
}
