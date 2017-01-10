package lap_processor

import (
//  "log"
//  "sort"
  "strconv"
  "fmt"
)
//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor implementation for slicing data by size or condition
//    
//////////////////////////////////////////////////////////////////////

// const (
//   slice_count = iota
//   slice_delta
// )

// type LapSlicerOne struct {

//   res       float64
//   count     int64
//   is_first  bool

//   index     float64
// }

type LapSlicer struct {
  LapDataCommon

  count     int64
  max_count int64

  max_delta float64
  start     float64
  delta_field string

  //last      LapData
  elements  [] *LapData

  id        string
}
func (p LapSlicer) GetSize() string {
  return fmt.Sprint("data: %d",len(p.elements))
}

func (p LapSlicer) fillInfo(info *map[string]string) {
  (*info)["type"]="slice"
  (*info)["count"]=strconv.FormatInt(p.count,10)
  (*info)["max_count"]=strconv.FormatInt(p.max_count,10)
  (*info)["max_delta"]=strconv.FormatFloat(p.max_delta,'g',-1,64)
  (*info)["start"]=strconv.FormatFloat(p.start,'g',-1,64)
  (*info)["delta_field"]=p.delta_field
}


func (p *LapSlicer) send_n_clear() {
  length:=len(p.elements)
  for i:=0; i<length; i++ {
    //data := NewLapData(  `[`, p.Parent.Name, ``)
    //p.copy_out_fields(p.elements[i],&data)
    
    p.Parent.SendToAll(p.elements[i])
    LapLog("AGGR SEND: %+v",*p.elements[i])
  }
  // send EOD
  data := NewLapData(  `x`)//, p.Parent.Name, ``)
  p.Parent.SendToAll(&data)
  // clean!
  p.elements=p.elements[:0]
}


func (p *LapSlicer) Input(n *LapNode, d *LapData, src string) {
  p.Parent=n
  //LapLog("AGGD: GOT %v",d.Cmd)
  if d.Cmd == `x` {
    // EOD

    LapLog("SLICE: EOD!")
    p.send_n_clear()
  }else{
    p.elements=append(p.elements,d)
    //LapLog("Elements: %+v",p.elements)
    if p.max_count>0 {
      // do counting
      p.count+=1
      if p.count>=p.max_count {
        p.send_n_clear()
        p.count=0
      }
    }else{
      // do delta
      n,_:=d.GetNum(p.delta_field)
      if p.count==0 {
        // no data yet
        p.start=n
        p.count=1
      }else{
        //LapLog("DELTA: %v-%v=%v",n,p.start,n-p.start)
        if n-p.start>=p.max_delta {
          p.send_n_clear()
          p.count=0
        }
      }
    }
  }
}

func (p LapSlicer) StartWork(*LapNode, *LapData, string) {
}


func (p *LapSlicer) NewProcessor(d LapData) LapDataProcessor{
  var ok bool

  a:=new(LapSlicer)
  a.delta_field,ok=d.GetStr(`delta_field`)
  if ok {
    a.max_delta=d.ToNum(`delta`)
    a.max_count=0
  }else{
    a.max_count=int64(d.ToNum(`count`))
  }
  a.fill_out_fields(d)
  a.elements=make([]*LapData,0)

  //LapLog("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  return a
}
func (p *LapSlicer) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
