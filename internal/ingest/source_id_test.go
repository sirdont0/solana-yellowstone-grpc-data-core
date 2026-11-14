package ingest

import "testing"

func TestSourceIDChangesWALIdentity(t *testing.T) {
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	baseline := c.Identity()
	c.SourceID = "publicnode-a"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Identity() == baseline {
		t.Fatal("different source must get a different WAL identity")
	}
	c.SourceID = "../escape"
	if err := c.Validate(); err == nil {
		t.Fatal("unsafe source ID must be rejected")
	}
}
