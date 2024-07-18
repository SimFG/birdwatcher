package etcd

import (
	"context"
	"fmt"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/etcd/show"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
)

func SearchCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "search",
		Short: "search anything about milvus in the etcd meta data, normal: collection/partition/segment",
		Run: func(cmd *cobra.Command, args []string) {
			info, err := cmd.Flags().GetString("info")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			searchResult := searchAnything(cli, basePath, info)
			fmt.Println(searchResult)
		},
	}

	cmd.Flags().String("info", "", "the key info to search")
	return cmd
}

func searchAnything(cli clientv3.KV, basePath string, info string) string {
	// 1. check the info is id or name
	// 2. collection/partition/segment
	// 3. display info
	// db-id/collection-id/name/creation-time/
	// field/vchannel/position/valid segments/segment rows/segment path

	searchResult, err := searchCollection(cli, basePath, info)
	if err == nil {
		return searchResult
	}

	return err.Error()
}

func searchCollection(cli clientv3.KV, basePath string, info string) (string, error) {
	infoID, err := strconv.ParseInt(info, 10, 64)
	isID := err == nil
	var collection *models.Collection
	ctx := context.Background()
	stringBuilder := &strings.Builder{}

	if isID {
		collection, err = common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), infoID)
		if err != nil {
			return "", err
		}
	} else {
		collections, err := common.ListCollectionsVersion(ctx, cli, basePath, etcdversion.GetVersion(), func(coll *models.Collection) bool {
			if coll.Schema.Name != info {
				return false
			}
			return true
		})
		if err != nil {
			return "", err
		}
		if len(collections) == 0 {
			return "", fmt.Errorf("collection not found, name: %s", info)
		}
		if len(collections) > 1 {
			fmt.Fprintln(stringBuilder, "there are more than one collection with the same name, please use id instead")
			for i, m := range collections {
				fmt.Fprintln(stringBuilder, i, ":", m.ID, ", state:", m.State.String())
			}
			return stringBuilder.String(), nil
		}
		collection = collections[0]
	}

	show.PrintCollection(stringBuilder, collection)

	segments, err := common.ListSegmentsVersion(ctx, cli, basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
		return segment.CollectionID == collection.ID && IsHealthSegment(segment.State)
	})
	if err != nil {
		fmt.Fprintln(stringBuilder, "failed to list segments, ", err.Error())
		return stringBuilder.String(), nil
	}

	fmt.Fprintln(stringBuilder, "--------------------------------------------------------")

	totalRC := int64(0)
	var growing, sealed, flushed int

	for _, segmentInfo := range segments {
		totalRC += segmentInfo.NumOfRows
		switch segmentInfo.State {
		case models.SegmentStateGrowing:
			growing++
		case models.SegmentStateSealed:
			sealed++
		case models.SegmentStateFlushing, models.SegmentStateFlushed:
			flushed++
		}
		fmt.Fprintf(stringBuilder, "SegmentID: %d State: %s, Level: %s, Row Count:%d, Deleted Row: %d \n",
			segmentInfo.ID, segmentInfo.State.String(),
			segmentInfo.Level.String(), segmentInfo.NumOfRows,
			GetDeltaLogCount(segmentInfo.GetDeltalogs()))
	}
	fmt.Printf("--- Growing: %d, Sealed: %d, Flushed: %d\n", growing, sealed, flushed)

	return stringBuilder.String(), nil
}

func IsHealthSegment(state models.SegmentState) bool {
	return state == models.SegmentStateGrowing ||
		state == models.SegmentStateSealed ||
		state == models.SegmentStateFlushing ||
		state == models.SegmentStateFlushed
}

func GetDeltaLogCount(deltaLogs []*models.FieldBinlog) int {
	count := 0
	for i := range deltaLogs {
		deltaLog := deltaLogs[i]
		for i2 := range deltaLog.Binlogs {
			count += int(deltaLog.Binlogs[i2].EntriesNum)
		}
	}
	return count
}
