package lap_processor

import (
//  "log"
//  "sort"
  "strings"
  "fmt"
  "strconv"
//  "math"
  "bytes"
  "../helpers"
)
//////////////////////////////////////////////////////////////////////
//    
// LapDataProcessor implementation for joining multiple data streams
//    
//////////////////////////////////////////////////////////////////////

type join_element struct {
  data LapData
  got  []bool
}

func new_join_element(n int,d ...LapData) join_element {
  je:=join_element{NewLapData(`[`), make([]bool,n)}
  for i:=range(je.got){ je.got[i]=false}
  if len(d)>0 {je.data=*(d[0].Copy())}
  return je
}

func (p join_element) is_ready() bool {
  for _,val:=range(p.got){
    if val==false {return false}
  }
  return true
}

type LapJoin struct {
  LapDataCommon

  by_fields []string // fields to be uniq in joining
  maps      map[string][]string  // source -> source_field,new_field
  sources   []string

  elements    map[string]join_element
  got_eod     []bool
  got_finish  []bool

  id        string

  count int64
}

func (p LapJoin) GetSize() string {
  str:=len(p.by_fields)+len(p.got_eod)+len(p.got_finish)
  data:=len(p.elements)
  for e,_:=range(p.maps){
    data+=len(e)
  }
  return fmt.Sprintf("str=%d, data=%d",str,data)
}

func (p LapJoin) fillInfo(info *map[string]string) {
  (*info)["type"]="join"
  (*info)["count"]=strconv.FormatInt(p.count,10)
  (*info)["by_fields"]=helpers.Reduce(p.by_fields,"",
    func(mem interface{},val interface{}) interface{} {
        if mem.(string) == "" {
          return val
        }
        return mem.(string)+","+val.(string)
      }).(string)
  (*info)["sources"]=helpers.Reduce(p.sources,"",
    func(mem interface{},val interface{}) interface{} {
        if mem.(string) == "" {
          return val
        }
        return mem.(string)+","+val.(string)
      }).(string)
  (*info)["got_eod"]=helpers.Reduce(p.got_eod,"",
    func(mem interface{},val interface{}) interface{} {
        if mem.(string) == "" {
          return strconv.FormatBool(val.(bool))
        }
        return mem.(string)+","+strconv.FormatBool(val.(bool))
      }).(string)
  (*info)["got_finish"]=helpers.Reduce(p.got_finish,"",
    func(mem interface{},val interface{}) interface{} {
        if mem.(string) == "" {
          return strconv.FormatBool(val.(bool))
        }
        return mem.(string)+","+strconv.FormatBool(val.(bool))
      }).(string)
}

func (p *LapJoin) OnParentFinish(data *LapData, from string) bool{
  index:=-1
  for i,str:=range(p.sources){
    if str==from {index=i;break;}
  }
  if index==-1 {LapLog("Undexpected... Skip."); return false}
  p.got_finish[index]=true
  LapLog("FINISHED: %+v",p.got_finish)
  for _,fin:=range(p.got_finish){ if fin==false {return false}}
  return true
}

func (p *LapJoin) send_n_del(index string) {
//  LapLog("JOIN SEND... '%v'=%+v",index,p.elements[index])
  e:=p.elements[index].data
  p.Parent.SendToAll(&e)

  delete(p.elements,index)
}

func (p *LapJoin) send_all() {
  for i,el := range(p.elements) {
    if el.is_ready() {
  //    LapLog("JOIN SEND: '%v'",el)
      p.send_n_del(i)
    // }else{
    //   LapLog("JOIN CANNOT SEND: '%v'",el)
    }
  }
  // clean it!
  p.elements=make(map[string]join_element)
}

// add new value from source to result
func (p *LapJoin) do_add(key string, d LapData, src string){
  //src:=d.From
  index:=-1
  for i,str:=range(p.sources){
    if str==src {index=i;break;}
  }

  if index==-1 {
    LapLog("JOIN ERROR: bad source: '%s' (%+v)",src,d)
    return
  }
  n,ok:=d.GetNum(p.maps[src][0])
  if ok {
    dd:=p.elements[key].data
    dd.AddNumber(p.maps[src][1],n)
    p.elements[key].got[index]=true
    delete(p.elements[key].data.NumFields,p.maps[src][0])
    //LapLog("JOIN ADDED: '%s' %d from=%s %v (%s)",key,index,src,p.elements[key].data,p.maps[src][0])
  }else{
    s,ok2:=d.GetStr(p.maps[src][0])
    if ok2 {
      dd:=p.elements[key].data
      dd.AddString(p.maps[src][1],s)
      p.elements[key].got[index]=true
      delete(p.elements[key].data.StrFields,p.maps[src][0])
      //LapLog("JOIN ADDED: '%s' %d from=%s %v (%s)",key,index,src,p.elements[key].data,p.maps[src][0])
    }else{

      LapLog("JOIN ERROR: no requested field '%s' from source '%s' [%+v]",p.maps[src][0], src,d)
      return
    }
  }
}

func (p LapJoin) StartWork(*LapNode, *LapData, string) {
}


func (p *LapJoin) Input(n *LapNode, d *LapData, src string) {
  //p.Parent=n
  //LapLog("JOIN: GOT %+v (from=%s,to=%s,Cmd=%s)",d,src,n.Name,d.Cmd)
  if d.Cmd == `x` {
    // EOD

    LapLog("[%s] JOIN: EOD from %s!",p.Parent.Name,src)
    index:=-1
    for i,str:=range(p.sources){
      if str==src {index=i;break;}
    }
    if index==-1 {LapLog("JOIN: Undexpected EOD... Skip."); return}
    p.got_eod[index]=true
    LapLog("[%s] JOIN: EODS=%+v",p.Parent.Name,p.got_eod)
    for _,eod:=range(p.got_eod){ if eod==false {return}}

    p.send_all()
    for i,_:=range(p.got_eod){ p.got_eod[i]=false}
    data:=NewLapData(`x`)//,``,``)
    p.Parent.SendToAll(&data)
  }else{
    var buffer bytes.Buffer
    for _,field:=range(p.by_fields){
      buffer.WriteString(d.ToStr(field))
      buffer.WriteString(" ")
    }

    key:=buffer.String()
    if _,ok:=p.elements[key]; ok {
      // this element already exists, join to it!
      p.do_add(key,*d,src)
      //LapLog("JOIN: add elem[%s] from %s %+v -> %v",key,src,d,p.elements[key])
      if p.elements[key].is_ready() {
        p.send_n_del(key)
        p.count+=1
        if p.count>1{
          //LapLog("JOINS counter... %d",len(p.elements))
          p.count=0
        }
      }
    }else{
      // first arrival of this element
      p.elements[key]=new_join_element(len(p.sources),*d)
      //LapLog("JOIN: first elem[%s] %+v",key,d)
      p.do_add(key,*d,src)
    }
  }
}

func (p *LapJoin) NewProcessor(d LapData) LapDataProcessor{
  a:=new(LapJoin)

  bf,_:=d.GetStr(`by_fields`)
  a.by_fields=strings.Split(bf,`,`)
  sf,_:=d.GetStr(`sources`)
  a.sources=strings.Split(sf,`,`)

  a.elements=make(map[string]join_element)
  a.got_eod=make([]bool,len(a.sources))
  for i,_:=range(a.got_eod){ a.got_eod[i]=false}

  a.maps=make(map[string][]string)
  m,_:=d.GetStr(`maps`)
  for _,map_element:=range(strings.Split(m,`,`)) {
    list:=strings.Split(map_element,`/`)
    a.maps[list[0]]=make([]string,2)
    a.maps[list[0]][0]=list[1]
    a.maps[list[0]][1]=list[2]
  }

  a.fill_out_fields(d)

  LapLog("New JOIN: %+v",*a)
  return a
}

func (p *LapJoin) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
