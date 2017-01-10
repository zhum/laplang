package lap_processor

import (
//  "log"
  "sort"
  "fmt"
  "strconv"
)
//////////////////////////////////////////////////////////////////////
//    
// LapDataProcessor implementation for local sorting  data by one field
//    
//////////////////////////////////////////////////////////////////////

type LapSorter struct {
  LapDataCommon

  max_count int64
  sort_field string
  sort_descend bool

  elements  [] *LapData

  id        string
}

func (p LapSorter) GetSize() string {
  return fmt.Sprint("data: %d",int64(len(p.elements)))
}

func (p LapSorter) fillInfo(info *map[string]string) {
  (*info)["type"]="sort"
  (*info)["max_count"]=strconv.FormatInt(p.max_count,10)
  (*info)["sort_field"]=p.sort_field
  (*info)["sort_descend"]=strconv.FormatBool(p.sort_descend)
}

func (p *LapSorter) send_n_del0() {
  i:=0
  length:=len(p.elements)
  if p.sort_descend {i=length-1}
  p.Parent.SendToAll(p.elements[i])
//  LapLog("AGGR SEND: %+v",p.elements[i])
  if p.sort_descend {
    p.elements=p.elements[:length-1]
//    LapLog("New EL: %+v",p.elements)
  }else{
    p.elements=p.elements[1:]
//    LapLog("New EL: %+v",p.elements)
  }
}

func (p *LapSorter) send_all() {
  length:=len(p.elements)
  if p.sort_descend {
    for i := length-1; i >=0; i-- {
      p.Parent.SendToAll(p.elements[i])
      LapLog("AGGR FLUSH: %+v",p.elements[i])  
    }
  }else{
    for i := 0; i < length; i++ {
      p.Parent.SendToAll(p.elements[i])
      LapLog("AGGR FLUSH: %+v",p.elements[i])  
    }
  }
  
  p.elements=p.elements[:0]
}

func (p LapSorter) StartWork(*LapNode, *LapData, string) {
}


func (p *LapSorter) Input(n *LapNode, d *LapData, src string) {
  p.Parent=n
  //LapLog("AGGD: GOT %v",d.Cmd)
  if d.Cmd == `x` {
    // EOD

    //LapLog("SLICE: EOD!")
    p.send_all()
    p.Parent.SendToAll(d)
  }else{
    length:=len(p.elements)
    n,_:=d.GetNum(p.sort_field)
    index:=sort.Search(length,func(i int) bool{
        f,_:=p.elements[i].GetNum(p.sort_field)
        return f>n
      })
    //do insert
    p.elements=append(p.elements[:index],
      append([]*LapData{d},p.elements[index:]...)...)
    //LapLog("Elements: %+v",p.elements)

    // send if too many items
    if int64(length+1)>=p.max_count {
      p.send_n_del0()
    }
  }
}

func (p *LapSorter)NewProcessor(d LapData) LapDataProcessor{
  a:=new(LapSorter)

  a.sort_field,_=d.GetStr(`sort_field`)
  _,ok:=d.GetStr(`sort_descend`)
  if ok {
    a.sort_descend=true //FIXME! Check passed option!
  }else{
    a.sort_descend=false
  }
  a.max_count=int64(d.ToNum(`max_count`))
  if a.max_count<1 {a.max_count=32}

  a.fill_out_fields(d)
  a.elements=make([]*LapData,0)

  LapLog("New SORTER: %+v",*a)
  return a
}
func (p *LapSorter) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
