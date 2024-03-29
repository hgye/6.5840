package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	// "time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }



// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// rpc for new task
		task := CallTaskReq()
		fmt.Println("task is ", task)
		switch task.Phase {
		case MAP_PHASE:
			err := doMapTask(task, mapf)
			args := TaskAckArgs{
				Id: os.Getegid(),
				Filename: task.Filename,
				Phase: task.Phase,
			}
			if err != nil {
				args.Succ = false
			} else {
				args.Succ = true
			}

			CallTaskAck(&args)

		case REDUCE_PAHSE:
			doReduceTask(task, reducef)
		}

		time.Sleep(time.Second * 5)

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMapTask(task *TaskReqReply, mapf func(string, string) []KeyValue) error {

	file, err := os.Open(task.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %s", task.Filename)
		return fmt.Errorf("cannot open %s", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
		return fmt.Errorf("cannot read %s", task.Filename)
	}

	kva := mapf(task.Filename, string(content))

	kvaLst := splitKva(kva, task.NReduce)
	// kvaLst is ready to write to file
	err = writeIntermediateFiles(kvaLst, task.WorkerId)
	if err != nil {
		// notify coordinator
		log.Fatal("cannot write intermediate file")
		return fmt.Errorf("cannot write intermediate file")
	}
	return nil
}

func doReduceTask(task *TaskReqReply, reducef func(string, []string) string) {

	intermediate := []KeyValue{}

	for _, fname := range task.Intermediate {
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("can't open file %v", fname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.WorkerId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func splitKva(kva []KeyValue, n int) *[]ByKey {
	kvaLst := make([]ByKey, n)
	for _, kv := range kva {
		idx := ihash(kv.Key) % n
		kvaLst[idx] = append(kvaLst[idx], kv)
	}
	return &kvaLst
}

func writeIntermediateFiles(kvalst *[]ByKey, id int) error {

	for idx, kva := range *kvalst {
		f, err := os.OpenFile(
			fmt.Sprintf("mr-%d-%d", id, idx),
			os.O_CREATE|os.O_APPEND|os.O_RDWR,
			0644)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(f)
		err = enc.Encode(&kva)
		if err != nil {
			fmt.Println(err)
			return err
		}
		f.Close()
	}
	return nil
}

func CallTaskReq() *TaskReqReply {
	args := TaskReqArgs{
		Id: os.Getegid(),
	}

	reply := TaskReqReply{}
	ok := call("Coordinator.TaskReq", &args, &reply)
	if !ok {
		fmt.Printf("call failed\n")
		// failed to communicate with coordinator, just exit
		os.Exit(0)
	}
	fmt.Println("reply is ", reply)
	return &reply
}

func CallTaskAck(args *TaskAckArgs) {
	reply := TaskAckReply{}

	ok := call("Coordinator.TaskAck", args, &reply)
	if !ok {
		fmt.Printf("call failed\n")
		// failed to communicate with coordinator, just exit
		os.Exit(0)

	}
}

func CallThenMapReduce() chan<- []string {
	ch := make(chan []string)
	go func() {
		r := <-ch
		args := ResultArgs{Out: r}
		reply := ResultReply{}
		ok := call("Coordinator.Result", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
		}

	}()
	return ch
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
