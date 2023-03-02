package sealer

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type taskSelector struct {
	best []storiface.StorageInfo //nolint: unused, structcheck
}

func newTaskSelector() *taskSelector {
	return &taskSelector{}
}

func (s *taskSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd SchedWorker) (bool, bool, error) {
	tasks, err := whnd.TaskTypes(ctx)
	if err != nil {
		return false, false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	_, supported := tasks[task]

	return supported, false, nil
}

func (s *taskSelector) Cmp(ctx context.Context, _ sealtasks.TaskType, a, b SchedWorker) (bool, error) {
	atasks, err := a.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}

	btasks, err := b.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if len(atasks) != len(btasks) {
		return len(atasks) < len(btasks), nil // prefer workers which can do less
	}

	return a.Utilization() < b.Utilization(), nil
}

var _ WorkerSelector = &taskSelector{}

func (s *taskSelector) FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, spt abi.RegisteredSealProof, whnd *WorkerHandle) bool {
	tasks, err := whnd.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false
	}
	if _, supported := tasks[task]; !supported {
		return false
	}

	paths, err := whnd.workerRpc.Paths(ctx)
	if err != nil {
		return false
	}

	have := map[storiface.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	for _, info := range s.best {
		if info.Weight != 0 { // 为0的权重是fecth来的，不是本地的
			if _, ok := have[info.ID]; ok {
				return true
			}
		}
	}

	return false
}
