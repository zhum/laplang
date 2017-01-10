package main

import . "./lap_processor"

//import log "github.com/ngmoco/timber"
//import "github.com/davecheney/profile"

import "runtime/pprof"

import (
	//  "log"
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/gorilla/handlers"
	_ "github.com/mkevac/debugcharts"
)

type tst struct {
	fl float64
	ar []int
	mp map[string]string
}

var HeadNode *LapNode
var http_port int = 7878
var debug_port int = 7890

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var do_mem_stat = false

///////////////////////////////////////////////////////////////////
func main() {
	//var node
	//var data *LapData=nil
	//runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	// localhost:7890/debug/charts
	go http.ListenAndServe(fmt.Sprintf(":%d", debug_port), handlers.CompressHandler(http.DefaultServeMux))
	//  defer profile.Start(profile.CPUProfile().Stop()
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			//log.Fatal(err)
			LapLog("ooops '%s'", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	LapInit()
	ch1 := NewLapChannel()

	go func() {
		var m runtime.MemStats
		for {
			time.Sleep(2 * time.Second)
			if !do_mem_stat {
				continue
			}
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > 2.4E+09 {
				panic("Too much space!")
			}
			LapLog("Used: %E; Sys: %E, HeapUse: %E, HeapAll: %E, Obj: %d, Numgc: %d",
				float64(m.Alloc),
				float64(m.Sys),
				float64(m.HeapAlloc),
				float64(m.HeapSys),
				m.HeapObjects, m.NumGC)
			for name, HeadNode := range GetAllNodes() {
				n := HeadNode.Node.DataProc
				if n == nil {
					LapLog("Node '%s': size=%s", name, "NONE")
				} else {
					LapLog("Node '%s': size=%s", name, (*n).GetSize())
				}
			}
			runtime.GC()
		}
	}()
	// tt:=tst{
	//   123.77,
	//   []int{1,2,3},
	//   make(map[string]string),
	// }
	// tt.mp[`qwe`]=`asd`
	// tt.mp[`zxc`]=`123123zxczxc`
	// LapLog("%s",StrAny(``,`..`,tt))
	// return
	//ch2:=make(chan LapData)

	//writer, _ := log.NewFileWriter("test.log")
	//formatter := log.NewPatFormatter("[%D %T] [%L] %-10x %M")
	//log.AddLogger(log.ConfigLogger{LogWriter: writer,
	//              Level:     log.FINEST,
	//              Formatter: formatter})
	LapLog("Started")

	HeadNode = NewLapNode(`head`, ``, ch1)

	control := flag.Arg(0)
	if control == `` {
		control = `control.txt`
	}
	read_control(HeadNode, control)

	Lap_http_start(HeadNode, http_port)

	//avg:=new(LapAvgCount)

	//avg.Count_field=`value`
	//avg.Out_field=`value`
	//avg.OutFields= []string{`time`,`node`,`value`}

	//avg.Parent=node
	//node.DataProc=avg

	// if false {
	// 	HeadNode.Input(NewLapData(`n`, `head`, `head`,
	// 		`name`, `file`,
	// 		`type`, `file`,
	// 		`filename`, `in.txt`,
	// 		`headers`, `headers`))
	// 	HeadNode.Input(NewLapData(`n`, `file`, `file`,
	// 		`name`, `avg1`,
	// 		`type`, `avg`,
	// 		`out_field`, `value`,
	// 		`count_field`, `value`,
	// 		`out_fields`, `time,node,value`))
	// 	HeadNode.Input(NewLapData(`n`, `avg1`, `avg1`,
	// 		`name`, `printer`,
	// 		`type`, `print`,
	// 		`out_field`, `value`,
	// 		`count_field`, `value`,
	// 		`out_fields`, `time,node,value`))
	// 	// node.Input(NewLapData(`n`,`avg1`,`avg1`,
	// 	//   NewLapStr(`name`,`avg2`),NewLapStr(`type`,`print`),
	// 	//   NewLapStr(`out_field`,`value`),NewLapStr(`avg_field`,`value`),
	// 	//   NewLapStr(`out_fields`,`time,node,value`)))

	// 	HeadNode.Input(NewLapData(`f`, `avg1`, `avg1`,
	// 		`name`, `printer`, `field`, `node`,
	// 		`filter`, `node-1,node2`))
	// }
	//    NewLapStr(`filter`,`[20200 100000]`)))

	//LapLog("$$$ %v",ListWaiters())

	go func() {
		defer DoneWaiter(HeadNode.Name)
		LapLog("Started %v", HeadNode.Name)
		HeadNode.Start()
		LapLog("Finished %v", HeadNode.Name)
	}()

	data := NewLapData(`s`, `to`, `*`, `starting`, `go!`)
	//data := NewLapData(`[`)
	HeadNode.Input(&data,``)
	//data = NewLapData(`x`, `to`, `head`)
	//HeadNode.Input(&data,``)

	//   count := 50
	//   count2 := 20000
	//   for i := 0; i < count; i++ {
	//     for j := 0; j < count2; j++ {
	//       // data:=NewLapData(`[`,`avg1`,`avg1`,
	//       //   `value`,float64(i+j),
	//       //   `time`,100+float64(i)*10+float64(j),
	//       //   `node`,fmt.Sprintf(`node-%02d`,int32(count)))
	//         data:=NewLapDataPairs(`[`,`avg1`,`avg1`,
	//           NewLapNum(`value`,float64(i+j)),
	//           NewLapNum(`time`,100+float64(i)*10+float64(j)),
	//           NewLapStr(`node`,fmt.Sprintf(`node-%02d`,int32(count))))
	// //        LapLog("=== %+v",data.Fields[`value`])
	//         node.Input(data)
	//       }
	//       //LapLog("Processing...")
	//       data:=NewLapData(`x`,`avg1`,`avg1`)
	//       node.Input(data)
	//       //LapLog("Done...")
	//     }
	//    time.Sleep(2*time.Second)
	//    data=NewLapData(`z`,`avg1`,`avg2`)
	//node.SendToAll(data)
	//LapLog("END!")

	// for{
	//   var list []string
	//   time.Sleep(100*time.Millisecond)
	//   list=ListWaiters()
	//   LapLog("$$$ %v",list)
	//   if list.len()==0 {
	//     break
	//   }
	// }
	WaitAll()
	LapLog("finished in %v seconds (not true...)", 3.9)
}

func read_control(node *LapNode, filename string) {

	fd, err := os.Open(filename)
	if err != nil {
		panic("Cannot read control file")
	}

	defer fd.Close()

	r := bufio.NewReader(fd)
	line, err := r.ReadString('\n')
	for err == nil {
		if line[0] == '#' {
			line, err = r.ReadString('\n')
			continue
		}
		fields := strings.Split(line, `;`)

		if len(fields) > 2 {
			data := NewLapData(fields[0], `from`, fields[1], `to`, fields[2])
			i := 3
			for i+1 < len(fields) {
				data.AddString(fields[i], strings.TrimSpace(fields[i+1]))
				i += 2
			}
			if i!=len(fields) {
				LapLog("Warning!!! Missing parameter %s",fields[i])
			}
			node.Input(&data,``)
		}

		line, err = r.ReadString('\n')
	}
	if err != io.EOF {
		LapLog("Error while file reading: %s", err)
	}
}
