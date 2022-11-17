package mr

import (
	"context"
	"net"
	"sync"
	"time"

	api "github.com/Kazuki-Ya/gMapReduce/api"
	"google.golang.org/grpc"
)

const TaskTimeOut = 3
const (
	MapTask byte = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	InProgress byte = iota
	Idle
	Completed
)

type userConfig struct {
	files          []string
	numberOfReduce int64
}

type Task struct {
	Type     byte
	State    byte
	Index    int64
	File     string
	WorkerId int64
}

// This struct need to implement grpc server
type Master struct {
	mu             sync.Mutex
	mapTasks       []Task
	reduceTasks    []Task
	numberOfMap    int64
	numberOfReduce int64
	api.UnimplementedMapReduceServer
}

var _ api.MapReduceServer = (*Master)(nil)

func MakeMasterServer(files []string, nReduce int64) (*Master, error) {
	cfg := &userConfig{
		files:          files,
		numberOfReduce: nReduce,
	}
	m, err := setupGRPCMasterServer(cfg)
	if err != nil {
		return nil, err
	}
	return m, err
}

func setupGRPCMasterServer(cfg *userConfig) (*Master, error) {
	gsrv := grpc.NewServer()
	srv, err := makeMasterServer(cfg)
	if err != nil {
		return nil, err
	}
	api.RegisterMapReduceServer(gsrv, srv)

	l, err := net.Listen("tcp", ":8000")
	go func() {
		if err := gsrv.Serve(l); err != nil {
			gsrv.GracefulStop()
		}
	}()

	return srv, err
}

func makeMasterServer(cfg *userConfig) (*Master, error) {
	m := &Master{}
	m.numberOfMap = int64(len(cfg.files))
	m.numberOfReduce = cfg.numberOfReduce
	m.mapTasks = make([]Task, 0, m.numberOfMap)
	m.reduceTasks = make([]Task, 0, m.numberOfReduce)

	for i := int64(0); i < m.numberOfMap; i++ {
		mapTask := Task{Type: MapTask, State: Idle, Index: i, File: cfg.files[i], WorkerId: -1}
		m.mapTasks = append(m.mapTasks, mapTask)
	}

	for i := int64(0); i < m.numberOfReduce; i++ {
		reduceTask := Task{Type: ReduceTask, State: Idle, Index: i, File: "", WorkerId: -1}
		m.reduceTasks = append(m.reduceTasks, reduceTask)
	}

	return m, nil
}

func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.numberOfMap == 0 && m.numberOfReduce == 0
}

func (m *Master) TaskRequest(ctx context.Context, req *api.TaskRequestArgs) (
	*api.TaskRequestResponse, error) {
	m.mu.Lock()

	workerid := req.WorkerId
	var task *Task
	if m.numberOfMap > 0 {
		task = m.selectTask(m.mapTasks, workerid)
	} else if m.numberOfReduce > 0 {
		task = m.selectTask(m.reduceTasks, workerid)
	} else {
		task = &Task{Type: ExitTask, State: Completed, Index: -1, File: "", WorkerId: -1}
	}
	res := &api.TaskRequestResponse{}
	res.Filename = task.File
	res.TaskId = task.Index
	res.TaskType = []byte{task.Type}

	m.mu.Unlock()

	go m.waitTask(task)

	return res, nil
}

func (m *Master) GetnReduce(ctx context.Context, req *api.GetnReduceRequestArgs) (
	*api.GetnReduceRequestResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := &api.GetnReduceRequestResponse{}
	res.NReduce = int64(len(m.reduceTasks))

	return res, nil
}

func (m *Master) ReportTask(ctx context.Context, req *api.ReportTaskArgs) (
	*api.ReportTaskResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var task *Task
	taskType := req.TaskType[0]
	if taskType == MapTask {
		task = &m.mapTasks[req.TaskId]
	} else if taskType == ReduceTask {
		task = &m.reduceTasks[req.TaskId]
	} else {
		return nil, nil
	}

	if task.WorkerId == int64(req.WorkerId) && task.State == InProgress {
		task.State = Completed
		taskType = req.TaskType[0]
		if taskType == MapTask && m.numberOfMap > 0 {
			m.numberOfMap--
		} else if taskType == ReduceTask && m.numberOfReduce > 0 {
			m.numberOfReduce--
		}
	}
	res := &api.ReportTaskResponse{}
	res.CanExit = m.numberOfMap == 0 && m.numberOfReduce == 0

	return res, nil
}

func (m *Master) selectTask(taskList []Task, workerid int64) *Task {

	for i := 0; i < len(taskList); i++ {
		if taskList[i].State == Idle {
			task := &taskList[i]
			task.State = InProgress
			task.WorkerId = workerid
			return task
		}
	}

	return &Task{Type: NoTask, State: Completed, Index: -1, File: "", WorkerId: -1}
}

func (m *Master) waitTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(time.Minute * TaskTimeOut)
	m.mu.Lock()
	defer m.mu.Unlock()
	if task.State == InProgress {
		task.State = Idle
		task.WorkerId = -1
	}
}
