package lap_processor

import (
//  "sync"
//  "log"
  "time"
  //"encoding/json"
)

//import _ "net/http/pprof"

// type LapData struct {
// 	Cmd    string     // command ('[' for data)
// 	Id     int64      // almost uniq message ID
// 	From   string     // sender
// 	To     string     // receiver
// 	Fields map[string]LapField // data
// }

//  list of data processor fabrics
var LapDataProcessorList map[string] LapDataProcessor

func AddDataProcessor(name string, f LapDataProcessor){
  if LapDataProcessorList == nil {
    LapDataProcessorList=make(map[string] LapDataProcessor)
  }
  LapDataProcessorList[name]=f
}

////////////////////////////////////////////////////////////////////////////
//
// Command 'create new child'
//
////////////////////////////////////////////////////////////////////////////
type LapNewChildCommand struct {
}

// @proc - parent processor
// @d    - command with args
//
// d.name -> name for new node
// d.type -> data processor name
//
//
func (p *LapNewChildCommand) Input(node *LapNode, d *LapData) (string, bool) {
  var data_proc LapDataProcessor

	channel := NewLapChannel()//make(chan LapData,1024)

  new_node_name,ok:=d.GetStr(`name`)
  if !ok {
    LapLog("No new processor name")
    return ``,false
  }

  new_node:=NewLapNode(new_node_name, node.Name, channel)
  if new_node==nil {
    LapLog("Bad new node name '%s'",new_node_name)
    return ``,false
  }
  t,_:=d.GetStr(`type`)
  if dp_creator,ok := LapDataProcessorList[t]; ok {
    data_proc=dp_creator.NewProcessor(*d)
  } else{
    LapLog("No such processor type '%v'",t)
    return ``,false
  }
  
  //data_proc.SetId(time.Now().String())
  //LapLog("Fresh data_proc: %+v",data_proc)
  //!!  new_node.DataProc=&data_proc
  //LapLog("New node0: %s",StrAny(``,`  `,new_node))
  //!!  (*new_node.DataProc).SetParent(new_node)
  //new_node.DataProc.Parent=new_node
  //node_str,_:=json.MarshalIndent(new_node,`==`,`  `)
  new_node.AddProcessor(&data_proc)
  node.AddChild(new_node_name,channel)
  AddWaiter(new_node.Name)

  LapLog("New node: %s",StrAny(``,`  `,new_node))

  l,t:=ListNodes()
  LapLog("NODES LIST:\n%s",l)
  LapLog("FILTERS LIST: %s",t)

  go func(){
    defer DoneWaiter(new_node.Name)
    LapLog("Started %v",new_node.Name)
    new_node.Start()
    LapLog("Finished %v",new_node.Name)
  }()

  return `ok`,false //true
}

func (p *LapNewChildCommand) NewCommander() LapCommander{
  return new(LapNewChildCommand)
}


/////////////////////////////////////////////////////////////////////////////
//
// Command 'set filter on child'
//
////////////////////////////////////////////////////////////////////////////
type LapSetFilterCommand struct {
}

// @proc - parent processor
// @d    - command with args
//
// d.name -> name of target node
// d.filter -> filter string
// d.field -> filter field
//
func (p *LapSetFilterCommand) Input(node *LapNode, d *LapData) (string, bool) {
  target,_:=d.GetStr(`name`)
  field,_:=d.GetStr(`field`)
  filter,_:=d.GetStr(`filter`)
  node.AddChildFilter(target,field,filter)
  LapLog("Filter set done.")

  return `ok`,false
}
func (p *LapSetFilterCommand) NewCommander() LapCommander{
  return new(LapSetFilterCommand)
}

/////////////////////////////////////////////////////////////////////////////
//
// Command 'connect node to another' (redirect output 1st node to input 2nd)
//
////////////////////////////////////////////////////////////////////////////
type LapOutConnectCommand struct {
}

// @proc - parent processor
// @d    - command with args
//
// d.target -> name of target node (new child)
//
func (p *LapOutConnectCommand) Input(node *LapNode, d *LapData) (string, bool) {
  target,_:=d.GetStr(`out_to`)

  channel := NewLapChannel()//make(chan LapData,1024)
  node.AddChild(target,channel)
  LapLog("Node '%s' Connect to: '%s'",node.Name,target)
  for {
    all_nodes_mutex.Lock()
    LapLog("Node '%s' Connect to: '%s'...",node.Name,target)
    if _,ok:=GetAllNodes()[target]; ok {
      //LapLog("Node '%s' Connect to: '%s' ok!",node.Name,target)
      target_node:=GetAllNodes()[target].Node
      all_nodes_mutex.Unlock()
      //LapLog("Node '%s' Connect to: '%s' ok2",node.Name,target)
      target_node.AddParent(node.Name,channel)
      //LapLog("Node '%s' Connect to: '%s' ok3",node.Name,target)
      break
    }else{
      LapLog("NODES: '%+v' (%s)",GetAllNodes(),target)
      all_nodes_mutex.Unlock()
      time.Sleep(time.Second)
    }
  }
  LapLog("Node '%s' Connect to: '%s' OK",node.Name,target)
  return `ok`,false
}
func (p *LapOutConnectCommand) NewCommander() LapCommander{
  return new(LapOutConnectCommand)
}

/////////////////////////////////////////////////////////////////////////////
//
// Command 'delete node'
//
////////////////////////////////////////////////////////////////////////////
type LapDeleteCommand struct {
}

// @proc - parent processor
// @d    - command with args
//
func (p *LapDeleteCommand) Input(node *LapNode, d *LapData) (string, bool) {
  send_eod_str,_:=d.GetStr(`send_eod`)
  send_eod:=true
  if send_eod_str == `n` || send_eod_str == `false` || send_eod_str==`0` {
    send_eod=false
  }

  LapLog("Node '%s' delete",node.Name)
  node.Delete(send_eod)
  return `ok`,false
}
func (p *LapDeleteCommand) NewCommander() LapCommander{
  return new(LapDeleteCommand)
}

/////////////////////////////////////////////////////////////////////////////
//
// Command 'send EOD to node'
//
////////////////////////////////////////////////////////////////////////////
type LapEodCommand struct {
}

// @proc - parent processor
// @d    - command with args
//
func (p *LapEodCommand) Input(node *LapNode, d *LapData) (string, bool) {
  
  eod:=NewLapData(`x`)//,node.Name,node.Name)
  node.Input(&eod,node.Name)

  return `ok`,false
}
func (p *LapEodCommand) NewCommander() LapCommander{
  return new(LapEodCommand)
}


/////////////////////////////////////////////////////////////////////////////
//
// Command 'START node'
//
////////////////////////////////////////////////////////////////////////////
type LapStartCommand struct {
}


// @proc - parent processor
// @d    - command with args
//
func (p LapStartCommand) Input(node *LapNode, d *LapData) (string, bool) {
  
  LapLog("[%s] START!",node.Name)
  if node.DataProc!=nil {
    (*node.DataProc).StartWork(node, d,node.Name)
  }
  //data:=NewLapData(`[`)//,node.Name,node.Name)
  //data.AddNumber(`fake`,1.0)
  //node.Input(&data,node.Name)
  //node.Input(&d,node.Name)
  //node.SendToAll(&d)

  return `ok`,false
}
func (p *LapStartCommand) NewCommander() LapCommander{
  return new(LapStartCommand)
}

