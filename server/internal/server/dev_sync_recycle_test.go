package server

import "testing"

// TestRerunSubscriptionsAfterSyncSkipsOnlyRecyclingSupervisedWorkers pins that
// only a supervised worker about to be replaced skips the post-sync rerun: its
// WebSockets drain onto the replacement, which evaluates every subscription.
func TestRerunSubscriptionsAfterSyncSkipsOnlyRecyclingSupervisedWorkers(t *testing.T) {
	cases := []struct {
		worker    string
		recycling bool
		want      bool
	}{
		{worker: "", recycling: false, want: true},
		{worker: "", recycling: true, want: true},
		{worker: "1", recycling: false, want: true},
		{worker: "1", recycling: true, want: false},
	}
	for _, testCase := range cases {
		t.Setenv("GONVEX_RUNTIME_WORKER", testCase.worker)
		if got := rerunSubscriptionsAfterSync(testCase.recycling); got != testCase.want {
			t.Fatalf("worker=%q recycling=%v: rerun = %v, want %v", testCase.worker, testCase.recycling, got, testCase.want)
		}
	}
}
