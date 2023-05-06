// TODO: NOT PROCESSING ALL INTERMEDIATE BUCKETS
package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAP_PENDING        = 0
	MAP_IN_PROGRESS    = 1
	REDUCE_PENDING     = 2
	REDUCE_IN_PROGRESS = 3
	REDUCE_DONE        = 4
)

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	nFiles      int
	jobs        map[int]int
	nMapDone    int
	nReduceDone int
}

type FileLock struct {
	mu   sync.Mutex
	file os.File
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	reply.NReduce = c.nReduce
	reply.NFiles = c.nFiles
	pendingMapJob := getPendingMapJob(c)
	if pendingMapJob != -1 { // if there are still pending map jobs
		reply.Jobtype = "map"                   // set job type
		reply.NJob = pendingMapJob              // set job number
		c.jobs[pendingMapJob] = MAP_IN_PROGRESS // change job to pending
		time.AfterFunc(time.Second*10, func() { // create 10s timer
			if c.jobs[pendingMapJob] == MAP_IN_PROGRESS { // if the job is still in progress
				c.jobs[pendingMapJob] = MAP_PENDING // change it to pending
			}
		})
		//fmt.Printf("Assigned map job %d\n", pendingMapJob)
		return nil // return no error
	}
	if c.nMapDone < c.nFiles { // if we still haven't mapped all the files but there are no pending jobs
		reply.Jobtype = "wait" // ask the worker to wait for the other workers to finish
		reply.NJob = 0         // anything is ok here
		return nil             // return no error
	}
	pendingReduceJob := getPendingReduceJob(c)
	if pendingReduceJob != -1 { // if there are still pending reduce jobs
		reply.Jobtype = "reduce"                      // set job type
		reply.NJob = pendingReduceJob                 // set job number
		c.jobs[pendingReduceJob] = REDUCE_IN_PROGRESS // change job to pending
		time.AfterFunc(time.Second*10, func() {       // create 10s timer
			if c.jobs[pendingReduceJob] == REDUCE_IN_PROGRESS { // if the job is still in progress
				c.jobs[pendingReduceJob] = REDUCE_PENDING // change it to pending
			}
		})
		//fmt.Printf("Assigned reduce job %d\n", pendingReduceJob)
		return nil

	}
	if c.nReduceDone < c.nFiles { // if we still haven't reduced all the files but there are no pending jobs
		reply.Jobtype = "wait" // ask the worker to wait for the other workers to finish
		reply.NJob = 0         // anything is ok here
		return nil             // return no error
	}
	return nil
}

func (c *Coordinator) SendResult(args *SendResultArgs, reply *SendResultReply) error {
	if args.Jobtype == "map" {
		c.nMapDone++
		c.jobs[args.NJob] = REDUCE_PENDING
	}
	if args.Jobtype == "reduce" {
		if c.jobs[args.NJob] != REDUCE_DONE { // if the job is not done yet
			os.Rename("mr-out-"+strconv.Itoa(args.NJob)+".temp", "mr-out-"+strconv.Itoa(args.NJob)) // rename the temp file
			c.jobs[args.NJob] = REDUCE_DONE
			c.nReduceDone++
		} else { // if the job is done and the worker is sending results late
			os.Remove("mr-out-" + strconv.Itoa(args.NJob) + ".temp") // delete the temp file
		}
	}
	reply.Ok = true
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
	if c.nReduceDone == c.nFiles {
		time.Sleep(time.Second * 5)
		for i := 0; i < c.nFiles; i++ {
			os.Remove("mr-in-" + strconv.Itoa(i))
			for j := 0; j < c.nReduce; j++ {
				os.Remove("mr-out-" + strconv.Itoa(i) + "-" + strconv.Itoa(j))
			}
		}
	}
	return c.nReduceDone == c.nFiles
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nFiles = len(files)
	c.nReduce = nReduce
	c.jobs = make(map[int]int)
	c.nMapDone = 0
	c.nReduceDone = 0
	// atomize input files in the form in-X with X going from 0 to len(files) - 1
	atomizeFiles(files)
	// create the mapping jobs
	createMappingJobs(&c)
	c.server()
	return &c
}

func checkFatalError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func atomizeFiles(files []string) {
	for index, filename := range files {
		func() { // use anonymous function to be able to defer stuff
			srcFile, err := os.Open(filename)                          // open the source file name
			checkFatalError(err)                                       // check for errors
			defer srcFile.Close()                                      // defer closing the file
			destFile, err := os.Create("mr-in-" + strconv.Itoa(index)) // create the destination file name
			checkFatalError(err)                                       // check for errors
			defer destFile.Close()                                     // defer closing the file
			_, err = io.Copy(destFile, srcFile)                        // copy the file
			checkFatalError(err)                                       // check for errors
			err = destFile.Sync()                                      // commit the file to storage
			checkFatalError(err)                                       // check fo errors
		}()

	}
}

func createMappingJobs(c *Coordinator) {
	for i := 0; i < c.nFiles; i++ {
		c.jobs[i] = MAP_PENDING
	}
}

func getPendingMapJob(c *Coordinator) int {
	for k, v := range c.jobs {
		if v == MAP_PENDING {
			return k
		}
	}
	return -1
}

func getPendingReduceJob(c *Coordinator) int {
	for k, v := range c.jobs {
		if v == REDUCE_PENDING {
			return k
		}
	}
	return -1
}
