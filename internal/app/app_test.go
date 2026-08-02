package app

import "testing"

func TestRepairReportDescription(t *testing.T) {
	tests := []struct {
		name   string
		report RepairReport
		want   string
	}{
		{
			name:   "single manifest backfill",
			report: RepairReport{ManifestTSVTargets: 1, ManifestTSVBackfill: true},
			want:   "backfilled TSV manifest on 1 configured target",
		},
		{
			name:   "multiple manifest repairs",
			report: RepairReport{ManifestTSVTargets: 2},
			want:   "repaired TSV manifest publication on 2 configured targets",
		},
		{
			name:   "single secondary synchronization",
			report: RepairReport{SynchronizedTargets: 1},
			want:   "synchronized 1 lagging secondary target",
		},
		{
			name:   "combined repairs",
			report: RepairReport{ManifestTSVTargets: 2, SynchronizedTargets: 3},
			want:   "repaired TSV manifest publication on 2 configured targets; synchronized 3 lagging secondary targets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.report.Description(); got != tt.want {
				t.Fatalf("Description() = %q, want %q", got, tt.want)
			}
		})
	}
}
