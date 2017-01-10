//
// Processors for LapLang
//
package lap_processor

import (
  "fmt"
  "sync"
)

var wg sync.WaitGroup
var waiters map[string]bool

func LapInit(){
  waiters=make(map[string]bool)

  //  commands processors
  AddLapCommander(`n`,new(LapNewChildCommand))
  AddLapCommander(`f`,new(LapSetFilterCommand))
  AddLapCommander(`c`,new(LapOutConnectCommand))
  AddLapCommander(`v`,new(LapListAllCommand))
  AddLapCommander(`d`,new(LapDeleteCommand))
  AddLapCommander(`e`,new(LapEodCommand))
  AddLapCommander(`s`,new(LapStartCommand))

  // data processors
  AddDataProcessor(`avg`,new(LapAvgCount))
  AddDataProcessor(`min`,new(LapMinCount))
  AddDataProcessor(`max`,new(LapMaxCount))
  AddDataProcessor(`print`,new(LapPrint))
  AddDataProcessor(`file`,new(LapCSVRead))
  AddDataProcessor(`http_csv`,new(LapHttpReadCSV))
  AddDataProcessor(`http_hopsa`,new(LapHttpReadHopsa))
  AddDataProcessor(`exec`,new(LapExecRead))
  AddDataProcessor(`outcsv`,new(LapCSVOut))
  AddDataProcessor(`null`,new(LapNullOut))
  AddDataProcessor(`slice`,new(LapSlicer))
  AddDataProcessor(`agr`,new(LapAggregateCount))
  AddDataProcessor(`sort`,new(LapSorter))
  AddDataProcessor(`grid`,new(LapGrid))
  AddDataProcessor(`join`,new(LapJoin))
}

func AddWaiter(str ... string) {
  LapLog("!!!%+v",str)
  if str!=nil {
    waiters[str[0]]=true
  }
  wg.Add(1)
}

func DoneWaiter(str ... string) {
  if str!=nil{
    delete(waiters,str[0])
  }
  wg.Done()
}

func WaitAll(){
  wg.Wait()
}

func ListWaiters() []string{
  keys := make([]string, 0, len(waiters))
  for k := range waiters {
    keys = append(keys, k)
  }
  return keys
}
// Commands:
// a = answer
// [ = data (csv)
// { = date (json)
// x = end of data


// logic incapsulation
type LapCommander interface {
  Input(*LapNode,*LapData) (string,bool)
  NewCommander() LapCommander
}

type LapFilter interface{
  Check(LapData) bool
  ToString() string
}

type LapStrFilter struct{
  Strings []string
  Field string
}
func (p LapStrFilter)Check(d LapData) bool {
  for _,str:=range p.Strings {
    //LapLog("Chk: %+v %v %v",d,p.Field,p.Strings[i])
    s,ok:=d.StrFields[p.Field]
    if ok && s == str {
      return true
    }
  }
  return false
}
func (p LapStrFilter)ToString() string {
  ret:="STR FILTER on "+p.Field+": "
  for _,str:=range p.Strings {
    ret+=str+";"
  }
  return ret
}

type LapNumFilter struct{
  Low, High float64
  Field string
}
func (p LapNumFilter)Check(d LapData) bool {
  n,ok:=d.NumFields[p.Field]
  if !ok {return false} // BAD DATA!
  if n <= p.High && n>= p.Low{
    return true
  }
  return false
}
func (p LapNumFilter)ToString() string {
  ret:=fmt.Sprintf("NUM FILTER on %s: %f..%f",p.Field,p.Low,p.High)
  return ret
}

var commands map[string] LapCommander

func AddLapCommander(n string,l LapCommander) {
  if commands == nil {
    commands=make(map[string]LapCommander)
  }
  commands[n]=l
}

