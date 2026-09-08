package promql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func SamplesFromModel(samples []model.SamplePair) []prompb.Sample {
	result := make([]prompb.Sample, 0, len(samples))
	for _, s := range samples {
		result = append(result, prompb.Sample{
			Value:     float64(s.Value),
			Timestamp: int64(s.Timestamp),
		})
	}
	return result
}

func ChunksFromModelSamples(samples []model.SamplePair) ([]storepb.AggrChunk, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	const samplesPerChunk = 120
	chunks := make([]storepb.AggrChunk, 0, (len(samples)+samplesPerChunk-1)/samplesPerChunk)
	for i := 0; i < len(samples); i += samplesPerChunk {
		end := i + samplesPerChunk
		if end > len(samples) {
			end = len(samples)
		}
		chunk, err := chunkFromModelSamples(samples[i:end])
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func chunkFromModelSamples(samples []model.SamplePair) (storepb.AggrChunk, error) {
	xorChunk := chunkenc.NewXORChunk()
	appender, err := xorChunk.Appender()
	if err != nil {
		return storepb.AggrChunk{}, fmt.Errorf("creating XOR chunk appender: %w", err)
	}

	minTime := int64(samples[0].Timestamp)
	maxTime := minTime
	for _, sample := range samples {
		timestamp := int64(sample.Timestamp)
		appender.Append(timestamp, float64(sample.Value))
		if timestamp < minTime {
			minTime = timestamp
		}
		if timestamp > maxTime {
			maxTime = timestamp
		}
	}
	xorChunk.Compact()

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: xorChunk.Bytes(),
		},
	}, nil
}

func TimeFromMillis(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond)).UTC()
}

func SeriesStep(request *storepb.SeriesRequest, fallback time.Duration) time.Duration {
	if request.QueryHints != nil && request.QueryHints.StepMillis > 0 {
		return time.Duration(request.QueryHints.StepMillis) * time.Millisecond
	}
	if request.Step > 0 {
		return time.Duration(request.Step) * time.Millisecond
	}
	if fallback > 0 {
		return fallback
	}
	return time.Minute
}

func SeriesStepForRange(request *storepb.SeriesRequest, fallback time.Duration, start, end time.Time, maxPoints int) time.Duration {
	step := SeriesStep(request, fallback)
	if maxPoints <= 0 || !end.After(start) {
		return step
	}

	minStep := minStepForMaxPoints(end.Sub(start), maxPoints)
	if minStep > step {
		return minStep
	}
	return step
}

func minStepForMaxPoints(rng time.Duration, maxPoints int) time.Duration {
	if maxPoints <= 0 || rng <= 0 {
		return 0
	}
	if maxPoints == 1 {
		return rng
	}

	divisor := time.Duration(maxPoints - 1)
	step := rng / divisor
	if rng%divisor != 0 {
		step++
	}
	return step
}

func ZLabelsFromPromLabels(lset labels.Labels) []labelpb.ZLabel {
	zls := make([]labelpb.ZLabel, 0, lset.Len())
	lset.Range(func(l labels.Label) {
		zls = append(zls, labelpb.ZLabel{Name: l.Name, Value: l.Value})
	})
	return zls
}

func ZLabelSetsFromPromLabels(lsets ...labels.Labels) []labelpb.ZLabelSet {
	res := make([]labelpb.ZLabelSet, 0, len(lsets))
	for _, ls := range lsets {
		res = append(res, labelpb.ZLabelSet{Labels: ZLabelsFromPromLabels(ls)})
	}
	return res
}

func ZLabelsToPromLabels(zls []labelpb.ZLabel) labels.Labels {
	if len(zls) == 0 {
		return labels.EmptyLabels()
	}
	builder := labels.NewBuilder(labels.EmptyLabels())
	for _, l := range zls {
		builder.Set(l.Name, l.Value)
	}
	return builder.Labels()
}

func SortStoreSeries(seriesSet []storepb.Series) {
	for i := range seriesSet {
		sort.Slice(seriesSet[i].Chunks, func(a, b int) bool {
			return seriesSet[i].Chunks[a].Compare(seriesSet[i].Chunks[b]) > 0
		})
	}
	sort.Slice(seriesSet, func(i, j int) bool {
		return labels.Compare(
			ZLabelsToPromLabels(seriesSet[i].Labels),
			ZLabelsToPromLabels(seriesSet[j].Labels),
		) < 0
	})
}

func SendStoreWarnings(server storepb.Store_SeriesServer, warnings v1.Warnings) error {
	for _, warning := range warnings {
		if err := server.Send(storepb.NewWarnSeriesResponse(errors.New(warning))); err != nil {
			return err
		}
	}
	return nil
}

func ContainsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type indexedBucket struct {
	index int32
	count float64
}

func SampleHistogramToFloatHistogram(sh *model.SampleHistogram) *histogram.FloatHistogram {
	if sh == nil {
		return nil
	}

	fh := &histogram.FloatHistogram{
		Count: float64(sh.Count),
		Sum:   float64(sh.Sum),
	}

	var posBuckets []indexedBucket
	var negBuckets []indexedBucket
	var schema int32 = 0
	schemaDetected := false

	for _, b := range sh.Buckets {
		if b == nil {
			continue
		}
		cnt := float64(b.Count)
		lower := float64(b.Lower)
		upper := float64(b.Upper)

		if b.Boundaries == 3 {
			// Zero bucket
			fh.ZeroCount = cnt
			if upper > 0 {
				fh.ZeroThreshold = upper
			} else if lower < 0 {
				fh.ZeroThreshold = -lower
			}
			continue
		}

		if cnt <= 0 {
			continue
		}

		if !schemaDetected && lower > 0 && upper > lower {
			ratio := upper / lower
			if ratio > 1 {
				log2Ratio := math.Log2(ratio)
				if log2Ratio > 0 {
					n := math.Round(-math.Log2(log2Ratio))
					if n >= -4 && n <= 8 {
						schema = int32(n)
						schemaDetected = true
					}
				}
			}
		}

		if lower >= 0 {
			var idx int32
			if lower > 0 {
				idx = int32(math.Round(math.Log2(lower) * math.Pow(2, float64(schema))))
			}
			posBuckets = append(posBuckets, indexedBucket{index: idx, count: cnt})
		} else if upper <= 0 {
			var idx int32
			if upper < 0 {
				idx = int32(math.Round(math.Log2(-upper) * math.Pow(2, float64(schema))))
			}
			negBuckets = append(negBuckets, indexedBucket{index: idx, count: cnt})
		}
	}

	fh.Schema = schema
	fh.PositiveSpans, fh.PositiveBuckets = buildSpansAndBuckets(posBuckets)
	fh.NegativeSpans, fh.NegativeBuckets = buildSpansAndBuckets(negBuckets)

	return fh
}

func buildSpansAndBuckets(buckets []indexedBucket) ([]histogram.Span, []float64) {
	if len(buckets) == 0 {
		return nil, nil
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].index < buckets[j].index
	})

	var spans []histogram.Span
	var counts []float64

	var currentSpan *histogram.Span
	var prevIndex int32

	for i, b := range buckets {
		counts = append(counts, b.count)
		if i == 0 {
			spans = append(spans, histogram.Span{
				Offset: b.index,
				Length: 1,
			})
			currentSpan = &spans[len(spans)-1]
			prevIndex = b.index
		} else if b.index == prevIndex+1 {
			currentSpan.Length++
			prevIndex = b.index
		} else {
			offset := b.index - (prevIndex + 1)
			spans = append(spans, histogram.Span{
				Offset: offset,
				Length: 1,
			})
			currentSpan = &spans[len(spans)-1]
			prevIndex = b.index
		}
	}

	return spans, counts
}

func SampleHistogramPairToProto(hp model.SampleHistogramPair) prompb.Histogram {
	fh := SampleHistogramToFloatHistogram(hp.Histogram)
	if fh == nil {
		return prompb.Histogram{}
	}
	ts := int64(hp.Timestamp)
	return prompb.FloatHistogramToHistogramProto(ts, fh)
}

func SampleHistogramToProto(s *model.Sample) prompb.Histogram {
	if s == nil || s.Histogram == nil {
		return prompb.Histogram{}
	}
	fh := SampleHistogramToFloatHistogram(s.Histogram)
	if fh == nil {
		return prompb.Histogram{}
	}
	ts := int64(s.Timestamp)
	return prompb.FloatHistogramToHistogramProto(ts, fh)
}

func ChunksFromModelHistogramSamples(samples []model.SampleHistogramPair) ([]storepb.AggrChunk, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	const samplesPerChunk = 120
	chunks := make([]storepb.AggrChunk, 0, (len(samples)+samplesPerChunk-1)/samplesPerChunk)
	for i := 0; i < len(samples); i += samplesPerChunk {
		end := i + samplesPerChunk
		if end > len(samples) {
			end = len(samples)
		}
		chunk, err := chunkFromModelHistogramSamples(samples[i:end])
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func chunkFromModelHistogramSamples(samples []model.SampleHistogramPair) (storepb.AggrChunk, error) {
	fhChunk := chunkenc.NewFloatHistogramChunk()
	appender, err := fhChunk.Appender()
	if err != nil {
		return storepb.AggrChunk{}, fmt.Errorf("creating FloatHistogram chunk appender: %w", err)
	}

	minTime := int64(samples[0].Timestamp)
	maxTime := minTime
	for _, sample := range samples {
		timestamp := int64(sample.Timestamp)
		fh := SampleHistogramToFloatHistogram(sample.Histogram)
		if fh != nil {
			appender.AppendFloatHistogram(nil, timestamp, fh, false)
		}
		if timestamp < minTime {
			minTime = timestamp
		}
		if timestamp > maxTime {
			maxTime = timestamp
		}
	}
	fhChunk.Compact()

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_FLOAT_HISTOGRAM,
			Data: fhChunk.Bytes(),
		},
	}, nil
}