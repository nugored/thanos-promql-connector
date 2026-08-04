package server

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"

	"main.go/pkg/promql"
)

type InfoAPIMode string

const (
	InfoAPIModeStore InfoAPIMode = "store"
	InfoAPIModeQuery InfoAPIMode = "query"
	InfoAPIModeBoth  InfoAPIMode = "both"
)

func ParseInfoAPIMode(value string, advertiseQueryAPI bool) (InfoAPIMode, error) {
	mode := InfoAPIMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		mode = InfoAPIModeStore
	}
	switch mode {
	case InfoAPIModeStore:
		if advertiseQueryAPI {
			return InfoAPIModeBoth, nil
		}
		return InfoAPIModeStore, nil
	case InfoAPIModeQuery, InfoAPIModeBoth:
		return mode, nil
	default:
		return "", fmt.Errorf("grpc-info-api-mode must be one of store, query, or both")
	}
}

func (mode InfoAPIMode) Effective() InfoAPIMode {
	if mode == "" {
		return InfoAPIModeStore
	}
	return mode
}

func (mode InfoAPIMode) AdvertisesStoreAPI() bool {
	mode = mode.Effective()
	return mode == InfoAPIModeStore || mode == InfoAPIModeBoth
}

func (mode InfoAPIMode) AdvertisesQueryAPI() bool {
	mode = mode.Effective()
	return mode == InfoAPIModeQuery || mode == InfoAPIModeBoth
}

type InfoServer struct {
	QueryBackend       string
	ExternalLabels     func() labels.Labels
	AnnouncedLabelSets func() []labels.Labels
	APIMode            InfoAPIMode
}

func (info *InfoServer) Info(ctx context.Context, in *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	labelSets := promql.ZLabelSetsFromPromLabels(labels.FromStrings("query-backend", info.QueryBackend))
	tsdbInfos := []infopb.TSDBInfo{{MinTime: math.MinInt64, MaxTime: math.MaxInt64}}
	usingFallbackLabelSet := true
	externalLabels := labels.EmptyLabels()
	if info.ExternalLabels != nil {
		externalLabels = info.ExternalLabels()
	}

	if info.AnnouncedLabelSets != nil {
		announcedLabelSets := info.AnnouncedLabelSets()
		if len(announcedLabelSets) > 0 {
			announcedLabelSets = extendLabelSets(announcedLabelSets, externalLabels)
			labelSets = promql.ZLabelSetsFromPromLabels(announcedLabelSets...)
			tsdbInfos = tsdbInfosFromLabelSets(labelSets)
			usingFallbackLabelSet = false
		}
	}

	if usingFallbackLabelSet {
		if externalLabels.Len() > 0 {
			labelSets = promql.ZLabelSetsFromPromLabels(externalLabels)
			tsdbInfos = tsdbInfosFromLabelSets(labelSets)
		}
	}

	componentType := component.Store.String()
	var queryInfo *infopb.QueryAPIInfo
	if info.APIMode.AdvertisesQueryAPI() {
		componentType = component.Query.String()
		queryInfo = &infopb.QueryAPIInfo{}
	}

	var storeInfo *infopb.StoreInfo
	if info.APIMode.AdvertisesStoreAPI() {
		storeInfo = &infopb.StoreInfo{
			MinTime:                      math.MinInt64,
			MaxTime:                      math.MaxInt64,
			SupportsWithoutReplicaLabels: true,
			TsdbInfos:                    tsdbInfos,
		}
	}

	return &infopb.InfoResponse{
		ComponentType: componentType,
		LabelSets:     labelSets,
		Store:         storeInfo,
		Query:         queryInfo,
	}, nil
}

func extendLabelSets(labelSets []labels.Labels, externalLabels labels.Labels) []labels.Labels {
	if externalLabels.IsEmpty() {
		result := make([]labels.Labels, 0, len(labelSets))
		for _, labelSet := range labelSets {
			result = append(result, labelSet.Copy())
		}
		return result
	}

	result := make([]labels.Labels, 0, len(labelSets))
	for _, labelSet := range labelSets {
		result = append(result, labelpb.ExtendSortedLabels(labelSet, externalLabels))
	}
	return result
}

func tsdbInfosFromLabelSets(labelSets []labelpb.ZLabelSet) []infopb.TSDBInfo {
	result := make([]infopb.TSDBInfo, 0, len(labelSets))
	for _, labelSet := range labelSets {
		result = append(result, infopb.TSDBInfo{
			Labels:  labelSet,
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		})
	}
	return result
}