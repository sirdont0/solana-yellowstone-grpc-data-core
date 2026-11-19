package ingest

import "fmt"

// ObserveRecord adapts durable lab Records to the coordinator. Unknown record
// kinds are ignored. In particular, slot, block_meta and block_footer never
// become MultiSourceBankClosed: the protobuf stream does not supply that proof.
// Callers must first persist records, preserving each source's receive order.
func (c *MultiSourceCoordinator) ObserveRecord(r Record) (MultiSourceDecision, error) {
	if r.Source == "" {
		switch r.Kind {
		case "session_end":
			c.mu.Lock()
			defer c.mu.Unlock()
			result := MultiSourceDecision{}
			for _, id := range c.order {
				s := c.sources[id]
				c.markOpenPartial(s, &result)
				if s.health.Connected || s.pendingAttempt {
					s.health.Connected = false
					s.pendingAttempt = false
					result.Processed = true
				}
			}
			if len(result.Issues) > 0 {
				result.Processed = true
			}
			result.Quality = c.quality()
			return result, nil
		case "continuity_broken", "filter_identity_changed", "checkpoint_write_failed", "dedupe_conflict":
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.globalFailure(r.Kind, r.Detail, r.Kind == "dedupe_conflict"), nil
		default:
			return MultiSourceDecision{Quality: c.currentQuality()}, nil
		}
	}
	o := MultiSourceObservation{SourceID: r.Source, Generation: r.Generation, Identity: r.Identity, Commitment: r.Commitment, FilterVersion: r.FilterVersion, Slot: r.Slot, BankID: r.BankID, ParentSlot: r.ParentSlot, Blockhash: r.Blockhash, TxIndex: r.TxIndex, Signature: r.Signature, Digest: r.Digest, Account: r.Account, WriteVersion: r.WriteVersion, Reason: r.Detail}
	switch r.Kind {
	case "connection_attempt":
		o.Kind = MultiSourceAttempt
	case "connection_open":
		o.Kind = MultiSourceConnect
	case "disconnect", "session_end":
		o.Kind = MultiSourceDisconnect
	case "continuity_broken", "replay_retention_exceeded", "replay_rejected", "dedupe_limit", "checkpoint_write_failed", "filter_identity_changed", "unreadable_update", "handler_error", "evidence_error":
		o.Kind = MultiSourceFailure
	case "dedupe_conflict":
		o.Kind = MultiSourceConflictSignal
	case "slot", "entry", "transaction_status":
		o.Kind = MultiSourceProgress
	case "block_meta":
		o.Kind = MultiSourceBlockMeta
	case "block_footer":
		o.Kind = MultiSourceBlockFooter
	case "transaction":
		o.Kind = MultiSourceTransaction
	case "account":
		o.Kind = MultiSourceAccount
	default:
		return MultiSourceDecision{Quality: c.currentQuality()}, nil
	}
	return c.Observe(o)
}

// ObserveWALRecord interprets a record inside a WAL owned by one source.
// Source-empty lifecycle markers affect only the WAL owner; they must not
// falsely disconnect or degrade independent providers sharing this coordinator.
func (c *MultiSourceCoordinator) ObserveWALRecord(walSource string, r Record) (MultiSourceDecision, error) {
	if r.Source != "" {
		if r.Source != walSource {
			return MultiSourceDecision{Quality: c.currentQuality()}, fmt.Errorf("WAL source mismatch: expected %s got %s", walSource, r.Source)
		}
		return c.ObserveRecord(r)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := MultiSourceDecision{Quality: c.quality()}
	s := c.sources[walSource]
	if s == nil {
		return result, fmt.Errorf("unknown WAL source %q", walSource)
	}
	switch r.Kind {
	case "session_end":
		// A graceful local writer close is not a bank-complete witness.
		// Preserve the uncertainty for every open bank owned by this WAL.
		c.markOpenPartial(s, &result)
		if len(result.Issues) > 0 {
			result.Processed = true
		}
		if s.health.Connected || s.pendingAttempt {
			s.health.Connected = false
			s.pendingAttempt = false
			result.Processed = true
		}
	case "continuity_broken", "filter_identity_changed", "checkpoint_write_failed", "replay_retention_exceeded", "replay_rejected", "dedupe_limit", "unreadable_update", "handler_error", "evidence_error":
		c.breakSource(s, &result, "wal_"+r.Kind, r.Slot, r.Detail)
		result.Processed = true
	case "dedupe_conflict":
		c.conflictSource(s, &result, "wal_"+r.Kind, r.Slot, r.Detail)
		result.Processed = true
	}
	result.Quality = c.quality()
	return result, nil
}

func (c *MultiSourceCoordinator) globalFailure(kind, reason string, conflict bool) MultiSourceDecision {
	result := MultiSourceDecision{Processed: true}
	for _, id := range c.order {
		s := c.sources[id]
		if conflict {
			c.conflictSource(s, &result, "global_"+kind, 0, reason)
		} else {
			c.breakSource(s, &result, "global_"+kind, 0, reason)
		}
	}
	result.Quality = c.quality()
	return result
}
