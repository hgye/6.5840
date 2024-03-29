package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MAP_PHASE Phase = iota
	REDUCE_PAHSE
)

type Phase int

type Coordinator struct {
	// Your definitions here.
	filemap map[string]State
	filech  chan Filestate
	nReduce int
	phase   Phase
	workers map[int]worker
	taskch  chan *task
}

type worker struct {
	// represent a worker
	id int
	// replych chan *TaskReqReply
	ackch chan *task
}

const (
	BEFORE_MAP State = iota
	PROCESS
	FININSH
)

type State int
type Filestate struct {
	name  string
	state State
}

type task struct {
	replych   chan *TaskReqReply
	oldWorker bool
	succ      bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Result(args *ResultArgs, reply *ResultReply) error {

	fmt.Printf("args is %v\n", args.Out)
	return nil
}

func (c *Coordinator) TaskReq(args *TaskReqArgs, reply *TaskReqReply) error {

	w, ok := c.workers[args.Id]
	if !ok {
		w = worker{
			ackch: make(chan *task),
		}
		c.workers[args.Id] = w
	}

	// centrel taskch, get reply parameters back

	replych := func() <-chan *TaskReqReply {
		ch := make(chan *TaskReqReply)
		c.taskch <- &task{
			oldWorker: ok,
			replych:   ch,
		}
		return ch
	}()
	r := <- replych
	reply.Filename = r.Filename
	reply.NReduce = r.NReduce
	reply.WorkerId = r.WorkerId
	fmt.Println("---reply is ", reply.WorkerId)

	// then run a goroutine for timeout(10s) recovery
	go func() {
		select {
		case <-time.After(time.Second * 10):
			// recover
			c.filech <- Filestate{
				name:  reply.Filename,
				state: BEFORE_MAP,
			}
		case t := <-w.ackch:
			if t.succ {
				c.filech <- Filestate{
					name:  reply.Filename,
					state: FININSH,
				}
			} else {
				c.filech <- Filestate{
					name:  reply.Filename,
					state: BEFORE_MAP,
				}
			}
		}
	}()
	return nil
}

func (c *Coordinator) TaskAck(args *TaskAckArgs, reply *TaskAckReply) error {
	w := c.workers[args.Id]

	w.ackch <- &task{
		succ: args.Succ,
	}

	reply = &TaskAckReply{}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	f := make(map[string]State, len(files))
	for _, v := range files {
		f[v] = BEFORE_MAP
	}

	c.filemap = f
	c.filech = make(chan Filestate)
	c.nReduce = nReduce
	c.phase = MAP_PHASE
	c.workers = make(map[int]worker, 10)
	taskch := make(chan *task)
	c.taskch = taskch

	go func() {
		cnt := 0 // not right
		for {
			t := <-taskch
			if !t.oldWorker {
				cnt += 1
			}
			var reply TaskReqReply
			switch c.phase {
			case MAP_PHASE:
				for file, state := range c.filemap {
					if state == BEFORE_MAP {
						reply.Filename = file
						c.filech <- Filestate{
							name:  file,
							state: PROCESS,
						}
						break
					}
				}
				reply.WorkerId = cnt
				reply.NReduce = c.nReduce
				t.replych <- &reply
			case REDUCE_PAHSE:
				fmt.Println("Enter reduce")

			}
		}
	}()

	// handle c.filemap and
	go func() {
		for {
			f := <-c.filech
			c.filemap[f.name] = f.state
			togger := true
			for _, s := range c.filemap{
				if s != FININSH {
					togger = false
					break
				}
			}
			if togger{
				c.phase = REDUCE_PAHSE
			}

		}
	}()
	c.server()
	return &c
}
