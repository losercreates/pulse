package engine

import (
	"context"
	"math"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// QueryRollup groups and dynamically re-aggregates window intervals into coarser time step buckets.
// Applies exact mathematical formulas: sum = sum(sum_i), count = sum(count_i), min = min(min_i),
// max = max(max_i), mean = sum / count.
func (e *ShardedMemoryEngine) QueryRollup(ctx context.Context, deviceID, metricName string, startNs, endNs, stepNs int64) ([]*telemetryv1.RollupBucket, error) {
	if stepNs <= 0 {
		stepNs = 60_000_000_000 // default 1 minute
	}

	records, err := e.QueryTimeSeries(ctx, deviceID, metricName, startNs, endNs, 100000)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return []*telemetryv1.RollupBucket{}, nil
	}

	var buckets []*telemetryv1.RollupBucket
	var currentBucket *telemetryv1.RollupBucket

	for _, rec := range records {
		bucketStart := (rec.WindowStartNs / stepNs) * stepNs
		bucketEnd := bucketStart + stepNs

		if currentBucket == nil || currentBucket.BucketStartNs != bucketStart {
			if currentBucket != nil {
				if currentBucket.Count > 0 {
					currentBucket.Mean = currentBucket.Sum / float64(currentBucket.Count)
				}
				buckets = append(buckets, currentBucket)
			}

			currentBucket = &telemetryv1.RollupBucket{
				BucketStartNs: bucketStart,
				BucketEndNs:   bucketEnd,
				Count:         rec.Count,
				Sum:           rec.Sum,
				Min:           rec.Min,
				Max:           rec.Max,
			}
		} else {
			currentBucket.Count += rec.Count
			currentBucket.Sum += rec.Sum
			currentBucket.Min = math.Min(currentBucket.Min, rec.Min)
			currentBucket.Max = math.Max(currentBucket.Max, rec.Max)
		}
	}

	if currentBucket != nil {
		if currentBucket.Count > 0 {
			currentBucket.Mean = currentBucket.Sum / float64(currentBucket.Count)
		}
		buckets = append(buckets, currentBucket)
	}

	return buckets, nil
}
