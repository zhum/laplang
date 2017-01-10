package lap_processor

import (
  "fmt"
  "time"
//  "runtime"
  "sync"
  "strings"
  "regexp"
  "strconv"
)

type data_n_src struct {
  data *LapData
  src  string
}

// General NODE. Procedsses all data and commands
// main method - Input
//
type LapNode struct {
  Name     string
  DataProc *LapDataProcessor

  //re_cmd, re_json, re_data, re_jparts, re_csv, re_csvparts *regexp.Regexp

  parents    map[string]chan *LapData
  childs     map[string]chan *LapData
  childFilters map[string] []LapFilter
  childMap   map[string]string

  parent_mutex sync.Mutex
  childs_mutex sync.Mutex

  id        int64

  in_count  int64
  out_count map[string]int64

  readData  []data_n_src
}

type LapNodeInfo struct{
  Node *LapNode
  parents []string
  childs  []string
}

var all_nodes map[string]*LapNodeInfo
var all_nodes_mutex = &sync.Mutex{}

func GetAllNodes() map[string]*LapNodeInfo {
  return all_nodes
}
//
//  Return true if this node should be finished
//
func (p *LapNode)OnParentFinish(data *LapData,from string) bool{
  return true
}

//
//  Return map with processor state, type and node name
//
func (p LapNode) getInfo() (map[string]string){
  ret:=make(map[string]string)

  ret["name"]=p.Name
  if p.DataProc != nil {
    (*p.DataProc).fillInfo(&ret)
  }else{
    ret["type"]="undefined"
  }
  return ret
}

func (p *LapNode) ReadDataFromParent() (*LapData, string, bool){

  allOpen:=true
  if len(p.readData)==0 {
    allOpen=p.readDataFromAllParents()
  }

  for i,data:=range p.readData {
    if data.data!=nil {
      if i==len(p.readData)-1{
        p.readData=p.readData[:0] // cut the list
      }else{
        p.readData[i].data=nil
      }
      //LapLog("{%s} GOT %+v",p.Name,*data)
      return data.data, data.src, allOpen
    }
  }
  return nil,``,true
}

func (p *LapNode) readDataFromAllParents() bool {

  allOpen:=true
  //p.parent_mutex.Lock()
  for via,p_chan := range p.parents{
    select {
      case data,ok := <- p_chan:
        if !ok {LapLog("[%s] Parent channel closed!",p.Name); allOpen=false; continue}

        if data.Cmd==`[`{
          p.in_count+=1
          //LapLog("[%s] Got %s from %s via %s",p.Name,data.Cmd, data.From, via)
        }else{
          //LapLog("[%s] Got %s via %s",p.Name,data.Cmd, via)
        }
        p.readData=append(p.readData,data_n_src{data,via})
      default:
      //case <- time.After(time.Second * 300):
          //LapLog("[%v] no data from %s", p.Name, via)
//        time.Sleep(1*time.Millisecond)
        //return nil,true
    }
  }
  //p.parent_mutex.Unlock()
  return allOpen
}


func (p LapNode) Start(){
  for{
    data,src,ok:=p.ReadDataFromParent()

    if ok {
      if data!=nil {
        if data.Cmd != `[` {LapLog("<------- Node %s: input=%s",p.Name,data)}

        // new data from parent!
        if data.Cmd==`z` {
          if p.OnParentFinish(data,src) {
            LapLog("Node '%s' finished!!!\n",p.Name)
            p.SendToAll(data)
          }
        }
        p.Input(data,src)
      }
    }else{
      break
    }
    //count++
//          LapLog("[%v] Got data", p.Name)
    //count:=0
    // if count>0 {
    // }
    time.Sleep(1*time.Microsecond)
    //runtime.Gosched()
  }
  LapLog("Node '%s' failed!!!\n",p.Name)
}

/////////////////////////////////////////////////////////////////////
//
//  @name  [string]: name of node
//  @pname [string]: name of parent node
//  @pchan [chan LapData]: channel to parent
//
func NewLapNode(name string, pname string, parent chan *LapData) *LapNode{

  if _,ok:=all_nodes[pname]; !ok {
    if pname!=`` {
      LapLog("No such parent node: '%s'", pname)
      return nil      
    }
  }

  p:= LapNode{Name: name}

  p.parents=make(map[string]chan *LapData)
  p.parent_mutex.Lock()
  p.parents[pname] = parent
  p.parent_mutex.Unlock()

  //p.Commands = make(map[string]LapCommander)
  p.childs   = make(map[string]chan *LapData)
  p.childMap = make(map[string]string)
  p.childFilters = make(map[string] []LapFilter)
  p.DataProc = nil
  p.out_count=make(map[string]int64)
  p.in_count=0
  p.id=node_counter
  node_counter+=1

  //p.compile_re()

  all_nodes_mutex.Lock()
  if all_nodes==nil {
    all_nodes=make(map[string]*LapNodeInfo)
  }
  all_nodes[name]=&LapNodeInfo{&p,make([]string,0),make([]string,0)}
  all_nodes[name].parents=append(all_nodes[name].parents,pname)
  all_nodes_mutex.Unlock()

  p.readData=make([]data_n_src,0)

  LapLog("NEW NODE: '%s', parent '%s', %v",name,pname,p)

  return &p
}

func (p LapNode) GetId() int64 {
  return p.id
}

func (p *LapNode) AddProcessor(data_proc *LapDataProcessor) {
  (*data_proc).SetParent(p)
  p.DataProc=data_proc
  LapLog("[%s] Processor added: %+v",p.Name,*(p.DataProc))
}

func (p LapNode) SendToAll(d *LapData) {
  //LapLog("[%v] Send to all. DATA=%+v\n",p.Name,d)
//  return
  //if d.To==`` {set_to=true}
  //if d.From==`` {d.From=p.Name}

  //!! RO p.childs_mutex.Lock()

CheckLoop:
  for name,c := range p.childs{
    if d.Cmd == `[` {//|| d.Cmd == `x`{
      if f,ok:=p.childFilters[name];ok {
        for _,filter:=range f {
          if ! filter.Check(*d){
            continue CheckLoop // do not send to this child
          }
        }
      }
    }
    //d2:=d //!! TODO: turn back copying
    //d2.To=name
    p.out_count[name]+=1
    //LapLog("    |--> from=%s,to=%s %+v ", p.Name,name,d)
    //if d2.Cmd != `[` {LapLog("---> from=%s,to=%s %+v ", d2.From,d2.To,d2)}
    if d.Cmd != `[` {LapLog("cmd ---> from=%s,to=%s %+v ", p.Name,name,d)}
    c <- d.Copy()
  }
  //!! RO p.childs_mutex.Unlock()
}

// Process incoming data or command
func (p *LapNode) Input(d *LapData, src string) {

  if d.Cmd == `[` || d.Cmd == `x` {
    if p.DataProc!=nil{
      (*p.DataProc).Input(p,d,src)
      return
    }
  }

  to,_:=d.GetStr(`to`)

  if d.Cmd == `n` {
    // new node!
    name,_:=d.GetStr(`name`)
    if to != p.Name {
      through,err:=p.searchChild(to)
      LapLog("[%s] NEW node req(%s): %s -> %s",p.Name,to,through,name)
      if err {
        LapLog("No such node - '%s'",to)
        return
      }
      p.childMap[name] = through  // lock?
    }
  }

  // send to childs?
  if to ==`*` || to == `` {  // to all
    for name,ch:=range p.childs{
      //new_data:=d
      //new_data.To=name
      LapLog("[%s] Send to *(%s) %s",p.Name,name,d.Cmd)
      ch <- d //&new_data
    }
  } else {  // forward or execute
    if to != p.Name {
      c, e := p.searchChild(to)
      if e {
        LapLog("[%v] CANNOT Send to '%s' '%s'",p.Name,to,d.Cmd)
        return
        panic(fmt.Sprintf("No Such child: %s\n", to))
      }
      LapLog("[%v] Forwarding to %s %+v",p.Name,c,d)
      //d2:=d
      p.childs[c] <- d //&d2
      LapLog("[%v] Forward DONE",p.Name)
      return
    }
  }

  // some command for me
  if _, ok := commands[d.Cmd]; ok {
    LapLog("[%v] Command: %v",p.Name,d.Cmd)
    answer,ok := commands[d.Cmd].Input(p,d)
    LapLog("[%v] Command: %v processed (%s)",p.Name,d.Cmd,answer)
    if ok {
      from,_:=d.GetStr(`from`)
      p.answer(answer,from,d.Id)
    }
  } else {
    if d.Cmd != `[` && d.Cmd != `x` {
      panic(fmt.Sprintf("No such command: %v", d.Cmd))
    }
  }

}

func (p LapNode) answer(a string,from string, id int64) {
  var data=NewLapData(`a`,`from`, p.Name, `to`, from)
  data.SetId(id)
  data.AddString(`value`,a)

  LapLog("[%s] Answer...",p.Name)
  if from == `` {
    p.parent_mutex.Lock()
    for _,v := range p.parents {
      v <- &data
    }
    p.parent_mutex.Unlock()
  }else{
    p.parents[from] <- &data
  }
}

// func (p *LapNode) AddCommand(s string, c LapCommander) {
//  p.Commands[s] = c
//  LapLog("Added command %s", s)
// }

// func (p *LapNode) DoProcess(d LapData) {
//  //TODO!
// }

func (p LapNode) searchChild(name string) (string, bool) {
  if v, ok := p.childMap[name]; ok {
    return v, false
  }
  return "", true
}

func (p LapNode) AddChild(name string, ch chan *LapData) {
  p.childs_mutex.Lock()
  p.childs[name] = ch
  p.childMap[name] = name
  all_nodes_mutex.Lock()
  all_nodes[p.Name].childs=append(all_nodes[p.Name].childs,name)
  all_nodes_mutex.Unlock()
  p.childs_mutex.Unlock()
  p.out_count[name]=0
}

func (p LapNode) AddChildFilter(name string, field string, f_str string) {
  var f LapFilter
  LapLog("[FILTER] node=%s setting filter for '%s'",p.Name,name)

  if len(f_str)==0 {
    LapLog("Warning!!! Empty filter. Ignore.")
    return
  }

  if _,ok:=p.childFilters[name]; !ok {
    p.childFilters[name]=make([]LapFilter, 0)
  }
  // interval
  if f_str[0] == '[' {
    re:=regexp.MustCompile(`^.([0-9.]+)\s+([0-9.]+)\]`)
    matches := re.FindAllStringSubmatch(f_str,-1)
    if matches==nil {
      LapLog("Bad match!")
      panic("Bad match!")
    }
    low,_:=strconv.ParseFloat(matches[0][1],64)
    high,_:=strconv.ParseFloat(matches[0][2],64)
    f=LapNumFilter{low,high,field}
  } else if f_str == `x` {
    delete(p.childFilters, name)
    LapLog("[FILTERS] %s set filter for '%s' (%v)",p.Name,name,f)

  }else{
    list:=strings.Split(f_str,`,`)
    f=LapStrFilter{list,field}
  }

  p.childFilters[name]=append(p.childFilters[name],f)
  LapLog("[FILTERS] %s set filter for '%s' (%v)",p.Name,name,f)
}

func (p LapNode) DelChildFilter(name string, field string) {
  var f LapFilter
  LapLog("[FILTER] node=%s deleting filter for '%s'",p.Name,name)

  delete(p.childFilters, name)
  LapLog("[FILTERS] %s set filter for '%s' (%v)",p.Name,name,f)
}

func (p LapNode) AddParent(name string, ch chan *LapData) {
  p.parent_mutex.Lock()
  p.parents[name] = ch
  all_nodes_mutex.Lock()
  all_nodes[p.Name].parents=append(all_nodes[p.Name].parents,name)
  all_nodes_mutex.Unlock()
  p.parent_mutex.Unlock()
}

func (p LapNode) ListChilds() []string {
  var ret []string
  ret=make([]string,0,len(p.childMap))

  p.childs_mutex.Lock()
  for i,_:= range p.childMap {
    ret=append(ret,i)
  }
  p.childs_mutex.Unlock()
  return ret
}

func (p LapNode) ListParents() []string {
  var ret []string
  ret=make([]string,0,len(p.parents))

  p.parent_mutex.Lock()
  for i,_:= range p.parents {
    ret=append(ret,i)
  }
  p.parent_mutex.Unlock()
  return ret
}

//
// Delete node
// @args:
//    send_eod:   bool [false] - send EOD to all childs before deletion
//
func (p LapNode) Delete(params ...interface{}) string {
  send_eod:=false

  for _, arg := range params {
    switch t := arg.(type) {
      case bool:
        send_eod=t
      default:
        panic("Unknown argument fo LapNode.Delete")
    }
  }

  if send_eod {
    z:=NewLapData(`x`)//,``,``)
    p.SendToAll(&z)
  }

  if p.DataProc != nil {
    LapLog("Stopping processor...")
    data:=NewLapData(`x`)
    (*p.DataProc).FinishWork(&p,&data,``)
  }

  // disconnect from parent
  all_nodes_mutex.Lock()
  for _,node:=range GetAllNodes() {
    delete(node.Node.childMap,p.Name)
  }
  for _,parent:=range GetAllNodes()[p.Name].parents {
    parent_info,ok:=GetAllNodes()[parent]
    if ok {
      parent_node:=parent_info.Node
      parent_node.childs_mutex.Lock()
      delete(parent_node.childs,p.Name)
      delete(parent_node.childFilters,p.Name)
      parent_node.childs_mutex.Unlock()

      new_childs:=make([]string,0)
      for _,c:=range parent_info.childs {
        if c!=p.Name {
          new_childs=append(new_childs,c)
        }
      }
      parent_info.childs=new_childs
      delete(parent_node.out_count,p.Name)
    }
  }
  all_nodes_mutex.Unlock()


  p.childs_mutex.Lock()
  p.parent_mutex.Lock()

  p.childs=make(map[string]chan *LapData)
  p.parents=make(map[string]chan *LapData)
  p.childFilters=make(map[string][]LapFilter)
  p.childMap=make(map[string]string)

  p.parent_mutex.Unlock()
  p.childs_mutex.Unlock()

  all_nodes_mutex.Lock()
  delete(all_nodes, p.Name)
  all_nodes_mutex.Unlock()


  return `ok`
}


/////////////////////////////////////////////
//
// Get json string like this: ['child1','child2']
// Represents this node childs
//
func (node LapNode) jsonNodeLinks() string{
  text:="["
  delimiter:=""

  for c,_:=range node.childs {
    text+=delimiter+`"`+c+`"`
    delimiter=","
  }

  return text+"]"
}

/////////////////////////////////////////////
//
// Get json string like this: '{child1: ['FILTER1','FILTER2'], ...}'
// Represents filters to childs
//
func (node LapNode) jsonNodeFilters() string{
  text:="{"
  delimiter:=""

  for child,f:=range node.childFilters {
    text+=delimiter+`"`+child+`": [`
    delimiter2:=""
    for _,filter:=range f {
      text+=delimiter2+`"`+filter.ToString()+`"`
      delimiter2=", "
    }
    text+="]"
    delimiter=", "
  }

  return text+"}"
}

/////////////////////////////////////////////
//
// Get json string like this: '{_in: 1000, child1: 10, ...}'
// Represents data counters (in/out)
//
func (node LapNode) jsonNodeCounters() string{
  text:=fmt.Sprintf("{\"_in\": %d",node.in_count)

  for child,count:=range node.out_count {
    text=fmt.Sprintf(`%s, "%s": %d`,text,child,count)
  }

  return text+"}"
}

/////////////////////////////////////////////
//
// Get json string like this: '{"type": "outcsv", "filename": "o.csv", ...}'
// Represents info about node (name, type, attributes)
//
func (node LapNode) jsonNodeInfo() string{
  //text:=fmt.Sprintf(`{"name": "%s`,node.Name)

  text:="{"
  delimiter:=""
  for name,value:=range node.getInfo() {
    text=fmt.Sprintf(`%s%s"%s": "%s"`,text,delimiter,name,value)
    delimiter=", "
  }

  return text+"}"
}
