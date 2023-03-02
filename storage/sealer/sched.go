package sealer

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type SchedWorker interface {
	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)
	Paths(context.Context) ([]storiface.StoragePath, error)
	Utilization() float64
}

type WorkerSelector interface {
	// Ok is true if worker is acceptable for performing a task.
	// If any worker is preferred for a task, other workers won't be considered for that task.
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a SchedWorker) (ok, preferred bool, err error)

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b SchedWorker) (bool, error) // true if a is preferred over b
}

type Scheduler struct {
	mctx context.Context // metrics context

	assigner Assigner

	workersLk sync.RWMutex

	Workers map[storiface.WorkerID]*WorkerHandle

	schedule       chan *WorkerRequest
	windowRequests chan *SchedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	SchedQueue  *RequestQueue
	OpenWindows []*SchedWindowRequest

	workTracker *workTracker

	info      chan func(interface{})
	rmRequest chan *rmRequest

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type WorkerHandle struct {
	workerRpc Worker

	Info storiface.WorkerInfo

	preparing *ActiveResources // use with WorkerHandle.lk
	active    *ActiveResources // use with WorkerHandle.lk

	lk sync.Mutex // can be taken inside sched.workersLk.RLock

	wndLk         sync.Mutex // can be taken inside sched.workersLk.RLock
	activeWindows []*SchedWindow

	Enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type SchedWindowRequest struct {
	Worker storiface.WorkerID

	Done chan *SchedWindow
}

type SchedWindow struct {
	Allocated ActiveResources
	Todo      []*WorkerRequest
}

type workerDisableReq struct {
	activeWindows []*SchedWindow
	wid           storiface.WorkerID
	done          func()
}

type WorkerRequest struct {
	Sector   storiface.SectorRef
	TaskType sealtasks.TaskType
	Priority int // larger values more important
	Sel      WorkerSelector
	SchedId  uuid.UUID

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	IndexHeap int
	ret       chan<- workerResponse
	Ctx       context.Context
}

type workerResponse struct {
	err error
}

type rmRequest struct {
	id  uuid.UUID
	res chan error
}

func newScheduler(ctx context.Context, assigner string) (*Scheduler, error) {
	var a Assigner
	switch assigner {
	case "", "utilization":
		a = NewLowestUtilizationAssigner()
	case "spread":
		a = NewSpreadAssigner()
	default:
		return nil, xerrors.Errorf("unknown assigner '%s'", assigner)
	}

	return &Scheduler{
		mctx:     ctx,
		assigner: a,

		Workers: map[storiface.WorkerID]*WorkerHandle{},

		schedule:       make(chan *WorkerRequest),
		windowRequests: make(chan *SchedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		SchedQueue: &RequestQueue{},

		workTracker: &workTracker{
			done:     map[storiface.CallID]struct{}{},
			running:  map[storiface.CallID]trackedWork{},
			prepared: map[uuid.UUID]trackedWork{},
		},

		info:      make(chan func(interface{})),
		rmRequest: make(chan *rmRequest),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}, nil
}

func (sh *Scheduler) Schedule(ctx context.Context, sector storiface.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &WorkerRequest{
		Sector:   sector,
		TaskType: taskType,
		Priority: getPriority(ctx),
		Sel:      sel,
		SchedId:  uuid.New(),

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		Ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *WorkerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.Ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

func (r *WorkerRequest) SealTask() sealtasks.SealTaskType {
	return sealtasks.SealTaskType{
		TaskType:            r.TaskType,
		RegisteredSealProof: r.Sector.ProofType,
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
	SchedId  uuid.UUID
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *Scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case rmreq := <-sh.rmRequest:
			sh.removeRequest(rmreq)
			doSched = true
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.SchedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.OpenWindows = append(sh.OpenWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())
		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.SchedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.OpenWindows = append(sh.OpenWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.Todo {
						sh.SchedQueue.Push(request)
					}
				}

				openWindows := make([]*SchedWindowRequest, 0, len(sh.OpenWindows))
				for _, window := range sh.OpenWindows {
					if window.Worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.OpenWindows = openWindows

				sh.workersLk.Lock()
				sh.Workers[req.wid].Enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *Scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.SchedQueue.Len(); sqi++ {
		task := (*sh.SchedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.Sector.ID,
			TaskType: task.TaskType,
			Priority: task.Priority,
			SchedId:  task.SchedId,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.OpenWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.Worker).String())
	}

	return out
}

type Assigner interface {
	TrySched(sh *Scheduler)
}

func (sh *Scheduler) trySched() {
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	done := metrics.Timer(sh.mctx, metrics.SchedAssignerCycleDuration)
	defer done()

	sh.assigner.TrySched(sh)
}

func (sh *Scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.Workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *Scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *Scheduler) removeRequest(rmrequest *rmRequest) {

	if sh.SchedQueue.Len() < 0 {
		rmrequest.res <- xerrors.New("No requests in the scheduler")
		return
	}

	queue := sh.SchedQueue
	for i, r := range *queue {
		if r.SchedId == rmrequest.id {
			queue.Remove(i)
			rmrequest.res <- nil
			go r.respond(xerrors.Errorf("scheduling request removed"))
			return
		}
	}
	rmrequest.res <- xerrors.New("No request with provided details found")
}

func (sh *Scheduler) RemoveRequest(ctx context.Context, schedId uuid.UUID) error {
	ret := make(chan error, 1)

	select {
	case sh.rmRequest <- &rmRequest{
		id:  schedId,
		res: ret,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sh *Scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sh *Scheduler) taskAddOne(wid storiface.WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.Workers[wid]; ok {
		whl.Info.TaskResourcesLk.Lock()
		defer whl.Info.TaskResourcesLk.Unlock()
		if counts, ok := whl.Info.TaskResources[phaseTaskType]; ok {
			counts.RunCount++
		}
	}
}

func (sh *Scheduler) taskReduceOne(wid storiface.WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.Workers[wid]; ok {
		whl.Info.TaskResourcesLk.Lock()
		defer whl.Info.TaskResourcesLk.Unlock()
		if counts, ok := whl.Info.TaskResources[phaseTaskType]; ok {
			counts.RunCount--
		}
	}
}

func (sh *Scheduler) getTaskCount(wid storiface.WorkerID, phaseTaskType sealtasks.TaskType, typeCount string) int {
	if whl, ok := sh.Workers[wid]; ok {
		if counts, ok := whl.Info.TaskResources[phaseTaskType]; ok {
			whl.Info.TaskResourcesLk.Lock()
			defer whl.Info.TaskResourcesLk.Unlock()
			if typeCount == "limit" {
				return counts.LimitCount
			}
			if typeCount == "run" {
				return counts.RunCount
			}
		}
	}
	return 0
}

func (sh *Scheduler) getTaskFreeCount(wid storiface.WorkerID, phaseTaskType sealtasks.TaskType) int {
	limitCount := sh.getTaskCount(wid, phaseTaskType, "limit") // json文件限制的任务数量
	runCount := sh.getTaskCount(wid, phaseTaskType, "run")     // 运行中的任务数量
	freeCount := limitCount - runCount

	if limitCount == 0 { // 0:禁止
		return 0
	}

	whl := sh.Workers[wid]
	log.Infof("worker %s %s: %d free count", whl.Info.Hostname, phaseTaskType, freeCount)

	if phaseTaskType == sealtasks.TTAddPiece {
		p1limitCount := sh.getTaskCount(wid, sealtasks.TTPreCommit1, "limit") // p1最大的任务数量
		p1runCount := sh.getTaskCount(wid, sealtasks.TTPreCommit1, "run")     // p1运行中的任务数量
		p1freeCount := p1limitCount - p1runCount                              // p1空闲任务数量
		if freeCount > 0 && p1freeCount > 0 {                                 // addpice空闲数量大于0，p1空闲任务数量大于0
			return p1freeCount
		}
		return 0
	}

	if phaseTaskType == sealtasks.TTPreCommit1 {
		if freeCount >= 0 {
			return freeCount
		}
		return 0
	}

	if phaseTaskType == sealtasks.TTPreCommit2 {
		c1runCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "run")
		c2runCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "run")
		if freeCount >= 0 && c1runCount <= 0 && c2runCount <= 0 { // 需做的任务空闲数量不小于0，且没有c1c2任务在运行
			return freeCount
		}
		log.Infof("worker already doing C1 or C2 taskjob")
		return 0
	}

	if phaseTaskType == sealtasks.TTCommit1 {
		p2runCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "run")
		c2runCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "run")
		if freeCount >= 0 && p2runCount <= 0 && c2runCount <= 0 { // 需做的任务空闲数量不小于0，且没有p2c2任务在运行
			return freeCount
		}
		log.Infof("worker already doing P2 or C2 taskjob")
		return 0
	}

	if phaseTaskType == sealtasks.TTCommit2 {
		p2runCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "run")
		c1runCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "run")
		if freeCount >= 0 && p2runCount <= 0 && c1runCount <= 0 { // 需做的任务空闲数量不小于0，且没有p2c1任务在运行
			return freeCount
		}
		log.Infof("worker already doing P2 or C1 taskjob")
		return 0
	}

	if phaseTaskType == sealtasks.TTFetch || phaseTaskType == sealtasks.TTFinalize ||
		phaseTaskType == sealtasks.TTUnseal || phaseTaskType == sealtasks.TTReadUnsealed { // 不限制
		return 1
	}

	return 0
}
