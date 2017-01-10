package lap_processor

import (
//  "log"
  "strconv"
)
//////////////////////////////////////////////////////////////////////
//    
//    Example of LapDataProcessor implementation
//    
//////////////////////////////////////////////////////////////////////
type LapAvgCount struct {
  LapDataCommon

  Count_field string
  Out_field string

  last      LapData
  sum       float64
  count     int64

  id        string
}

func (p *LapAvgCount) GetSize() string {
  return "const"
}

func (p *LapAvgCount) fillInfo(info *map[string]string) {
  (*info)["type"]="avg_count"
  (*info)["count_field"]=p.Count_field
  (*info)["out_field"]=p.Out_field
  (*info)["sum"]=strconv.FormatFloat(p.sum,'g',-1,64)
  (*info)["count"]=strconv.FormatInt(p.count,10)
}

func (p *LapAvgCount) Input(node *LapNode, d *LapData, src string) {
  p.Parent=node
  if d.Cmd == `x` {
    // EOD

    data := NewLapData(  `[`)//, p.Parent.Name, ``)

    var avg=p.sum/float64(p.count)
    //LapLog("sum=%v, count=%v, avg=%v\n",p.sum,p.count,avg)
    p.sum=0
    p.count=0

    p.copy_out_fields(&p.last,&data)
    data.AddNumber(p.Out_field,avg)
//    LapLog("Last data: %v",p.last)
    //LapLog("New data: %v, %+v",data, p.OutFields)
    p.Parent.SendToAll(&data)
  }else{
    // Count AVG
    LapLog("--- sum=%v, count=%v data=%s\n",p.sum,p.count,d)
    n,_:=d.GetNum(p.Count_field)
    //LapLog("--- sum=%v, count=%v data=%+v, n=%v\n",p.sum,p.count,d,n)
    p.sum+=n
    p.count+=1
    p.last=*d
  }
}
func (p *LapAvgCount) NewProcessor(d LapData) LapDataProcessor{
  a:=LapAvgCount{}
  a.Count_field,_=d.GetStr(`count_field`)
  a.Out_field,_=d.GetStr(`out_field`)
  a.fill_out_fields(d)

//  a.id="qweqwe"
//  LapLog("NEW AVGCOUNT %v",a.id)
  return &a
}
func (p *LapAvgCount) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}


//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor implementation of MIN
//    
//////////////////////////////////////////////////////////////////////
type LapMinCount struct {
  LapDataCommon

  Count_field string
  Out_field string

  last      LapData
  min       float64
  first     bool

  id        string
}
func (p *LapMinCount) GetSize() string {
  return "const"
}

func (p *LapMinCount) fillInfo(info *map[string]string) {
  (*info)["type"]="min_count"
  (*info)["count_field"]=p.Count_field
  (*info)["out_field"]=p.Out_field
  (*info)["min"]=strconv.FormatFloat(p.min,'g',-1,64)
}

func (p *LapMinCount) Input(node *LapNode, d *LapData, src string) {
  p.Parent=node
  if d.Cmd == `x` {
    // EOD

    data:= NewLapData(  `[`)//, p.Parent.Name, ``)

    p.first=true
    p.copy_out_fields(&p.last,&data)
    data.AddNumber(p.Out_field,p.min)
    LapLog("New min data: %v",data)
    p.Parent.SendToAll(&data)
  }else{
    // Count Min
    n,_:=d.GetNum(p.Count_field)
    if p.first {
      //LapLog("--- start value=%f\n",n)
      p.min=n
      p.first=false
    }else{
      //LapLog("+++ new value=%f min=%f\n",n,p.min)
      if p.min>n {
        p.min=n
      }
    }
    p.last=*d
  }
}
func (p *LapMinCount) NewProcessor(d LapData) LapDataProcessor{
  a:=LapMinCount{}
  a.Count_field,_=d.GetStr(`count_field`)
  a.Out_field,_=d.GetStr(`out_field`)
  a.fill_out_fields(d)
  a.first=true
  return &a
}
func (p *LapMinCount) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}

//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor implementation of MIN
//    
//////////////////////////////////////////////////////////////////////
type LapMaxCount struct {
  LapDataCommon

  Count_field string
  Out_field string

  last      LapData
  max       float64
  first     bool

  id        string
}
func (p *LapMaxCount) GetSize() string {
  return "const"
}

func (p *LapMaxCount) fillInfo(info *map[string]string) {
  (*info)["type"]="max_count"
  (*info)["count_field"]=p.Count_field
  (*info)["out_field"]=p.Out_field
  (*info)["max"]=strconv.FormatFloat(p.max,'g',-1,64)
}

func (p *LapMaxCount) StartWork(*LapNode, *LapData, string) {
}
func (p *LapMinCount) StartWork(*LapNode, *LapData, string) {
}
func (p *LapAvgCount) StartWork(*LapNode, *LapData, string) {
}

func (p *LapMaxCount) Input(node *LapNode, d *LapData, src string) {
  p.Parent=node
  if d.Cmd == `x` {
    // EOD

    data:= NewLapData(  `[`)//, p.Parent.Name, ``)

    p.first=true
    p.copy_out_fields(&p.last,&data)
    data.AddNumber(p.Out_field,p.max)
    LapLog("New max data: %v (last=%v) outfields: %+v",data,p.last,p.OutFields)
    p.Parent.SendToAll(&data)
  }else{
    // Count Max
    n,_:=d.GetNum(p.Count_field)
    if p.first {
      //LapLog("--- start value=%f\n",n)
      p.max=n
      p.first=false
    }else{
      if p.max<n {
        p.max=n
      }
    }
    p.last=*d
  }
}
func (p *LapMaxCount) NewProcessor(d LapData) LapDataProcessor{
  a:=LapMinCount{}
  a.Count_field,_=d.GetStr(`count_field`)
  a.Out_field,_=d.GetStr(`out_field`)
  a.fill_out_fields(d)
  a.first=true
  return &a
}
// func (p *LapMaxCount) SetId(s string){
//   p.id=s
//   LapLog("Updated id: %v",p)
// }

