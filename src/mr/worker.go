package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	for {
		// Your worker implementation here.
		jobType, nJob, nReduce, nFiles := CallGetJob()
		if jobType == "wait" { // if we have been asked to wait
			time.Sleep(time.Second * 5) // wait for five seconds
			continue                    // get another job
		}
		// MAPPING JOB
		if jobType == "map" { // if we need to do a map job
			//fmt.Printf("Received map job %d\n", nJob)
			filename := "mr-in-" + strconv.Itoa(nJob) // get the file name
			file, err := os.Open(filename)            // open the file (no need to lock as we're only reading from it)
			checkFatalError(err)                      // check for fatal error
			content, err := ioutil.ReadAll(file)      // read file content
			checkFatalError(err)                      // check for fatal error
			file.Close()                              // close the file
			kva := mapf(filename, string(content))    // get the key-value array

			var files []*os.File           // bucket files
			for i := 0; i < nReduce; i++ { // create nReduce bucket files
				filename := "mr-out-" + strconv.Itoa(nJob) + "-" + strconv.Itoa(i)
				newFile, err := os.Create(filename)                     // create new bucket file
				checkFatalError(err)                                    // check for fatal error
				defer newFile.Close()                                   // defer closing the file
				err = syscall.Flock(int(newFile.Fd()), syscall.LOCK_EX) // try to lock the file
				checkFatalError(err)                                    // check for fatal error
				defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)    // defer unlocking the file
				files = append(files, newFile)                          // append file to list
			}

			for _, kv := range kva {
				nBucket := ihash(kv.Key) % nReduce                         // get the bucket number
				files[nBucket].WriteString(kv.Key + " " + kv.Value + "\n") // write into appropriate bucket file
			}
			CallSendResult(jobType, nJob) // let the coordinator we're done with the job
			continue                      // get another job
		}
		// REDUCE JOB
		if jobType == "reduce" {
			//fmt.Printf("Received reduce job %d\n", nJob)
			baseFilename := "mr-out-"
			var filenames []string
			for i := 0; i < nFiles; i++ {
				filenames = append(filenames, baseFilename+strconv.Itoa(i)+"-"+strconv.Itoa(nJob))
			}
			var kva []KeyValue
			for _, filename := range filenames {
				file, err := os.Open(filename)
				checkFatalError(err)
				fileScanner := bufio.NewScanner(file)

				fileScanner.Split(bufio.ScanLines)

				for fileScanner.Scan() {
					line := fileScanner.Text()
					kv := strings.Split(line, " ")
					kva = append(kva, KeyValue{kv[0], kv[1]})
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(nJob) + ".temp"
			ofile, _ := os.Create(oname)
			err := syscall.Flock(int(ofile.Fd()), syscall.LOCK_EX) // try to lock the file
			checkFatalError(err)                                   // check for fatal error
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			syscall.Flock(int(ofile.Fd()), syscall.LOCK_UN) // unlock the file
			ofile.Close()                                   // close the file
			CallSendResult(jobType, nJob)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallGetJob() (string, int, int, int) {
	args := GetJobArgs{}
	reply := GetJobReply{}

	ok := call("Coordinator.GetJob", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return reply.Jobtype, reply.NJob, reply.NReduce, reply.NFiles
}

func CallSendResult(jobType string, nJob int) bool {
	args := SendResultArgs{}
	reply := SendResultReply{}
	args.Jobtype = jobType
	args.NJob = nJob
	ok := call("Coordinator.SendResult", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return true
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
