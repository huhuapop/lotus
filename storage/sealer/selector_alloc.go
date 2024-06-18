package sealer

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type allocSelector struct {
	index paths.SectorIndex
	alloc storiface.SectorFileType
	ptype storiface.PathType
	miner abi.ActorID
}

func newAllocSelector(index paths.SectorIndex, alloc storiface.SectorFileType, ptype storiface.PathType, miner abi.ActorID) *allocSelector {
	return &allocSelector{
		index: index,
		alloc: alloc,
		ptype: ptype,
		miner: miner,
	}
}

func (s *allocSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd SchedWorker) (bool, bool, error) {
	tasks, err := whnd.TaskTypes(ctx)
	if err != nil {
		return false, false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, false, nil
	}

	paths, err := whnd.Paths(ctx)
	if err != nil {
		return false, false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[storiface.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	ssize, err := spt.SectorSize()
	if err != nil {
		return false, false, xerrors.Errorf("getting sector size: %w", err)
	}

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype, s.miner)
	if err != nil {
		return false, false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	requested := s.alloc

	for _, info := range best {
		if _, ok := have[info.ID]; ok {
			requested = requested.SubAllowed(info.AllowTypes, info.DenyTypes)

			// got all paths
			if requested == storiface.FTNone {
				break
			}
		}
	}

	return requested == storiface.FTNone, false, nil
}

func (s *allocSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b SchedWorker) (bool, error) {
	return a.Utilization() < b.Utilization(), nil
}

var _ WorkerSelector = &allocSelector{}

func (s *allocSelector) FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, spt abi.RegisteredSealProof, whnd *WorkerHandle) bool {
	paths, err := whnd.workerRpc.Paths(ctx)
	if err != nil {
		return false
	}

	have := map[storiface.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	var ft storiface.SectorFileType
	switch task {
	case sealtasks.TTAddPiece:
		ft = 0
	case sealtasks.TTPreCommit1:
		ft = storiface.FTUnsealed
	case sealtasks.TTPreCommit2:
		ft = storiface.FTCache | storiface.FTSealed
	case sealtasks.TTCommit1:
		ft = storiface.FTCache | storiface.FTSealed
	case sealtasks.TTCommit2:
		ft = storiface.FTCache | storiface.FTSealed
	case sealtasks.TTFetch:
		ft = storiface.FTUnsealed | storiface.FTCache | storiface.FTSealed
	case sealtasks.TTFinalize:
		ft = storiface.FTCache | storiface.FTSealed
	}

	ssize, err := spt.SectorSize()
	if err != nil {
		return false
	}

	find, err := s.index.StorageFindSector(ctx, sid, ft, ssize, false)
	if err != nil {
		return false
	}

	for _, info := range find {
		if _, ok := have[info.ID]; ok {
			return true
		}
	}

	return false
}
