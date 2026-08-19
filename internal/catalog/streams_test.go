package catalog

import "testing"

func TestStreamNames(t *testing.T) {
	if GroupName("default", 3) != "shard3" || GroupName("eu", 0) != "eu-shard0" {
		t.Fatal("group names")
	}
	for name, ok := range map[string]bool{"orders": true, "a_1": true, "Orders": false, "1a": false, "": false, "a-b": false,
		"abcdefghijabcdefghijabcdefghijab": true, "abcdefghijabcdefghijabcdefghijabc": false} {
		if ValidStreamName(name) != ok {
			t.Errorf("ValidStreamName(%q) = %t", name, !ok)
		}
	}
	if got := StreamSlotName("orders", "eu-shard0"); got != "pgshard_orders_eu_shard0" {
		t.Fatal(got)
	}
	if got := StreamSlotName("x", "Shard.1"); got != "pgshard_x_shard_1" {
		t.Fatal(got)
	}
}
