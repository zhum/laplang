package lap_processor

import (
//  "log"
  "sort"
  "strconv"
  "fmt"
)
//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor implementation for grouping data by one field
//    and then agregation them with min/max/sum/avg
//    
//////////////////////////////////////////////////////////////////////

const (
  aggr_min = iota
  aggr_max
  aggr_avg
  aggr_sum
)

type LapAggregateCountOne struct {

  res       float64
  count     int64
  is_first  bool

  index     float64
}

type LapAggregateCount struct {
  LapDataCommon

  Count_field string
  Group_field string
  Out_field   string
  MaxBuf      int64

  last      LapData
  elements  []*LapAggregateCountOne
  aggregation_type byte

  id        string
}

func (p LapAggregateCount) fillInfo(info *map[string]string) {
  (*info)["type"]="aggregate"
  (*info)[`count_field`]=p.Count_field
  (*info)[`group_field`]=p.Group_field
  (*info)[`out_field`]=p.Out_field
  (*info)[`max_buf`]=strconv.FormatInt(p.MaxBuf,10)
  switch p.aggregation_type {
    case aggr_min:
      (*info)[`aggregation`]="min"
    case  aggr_max:
      (*info)[`aggregation`]="max"
    case  aggr_avg:
      (*info)[`aggregation`]="avg"
    case  aggr_sum:
      (*info)[`aggregation`]="sum"
    default:
      (*info)[`aggregation`]="unknown"
  }
}

func (p *LapAggregateCount) send_n_del0() {
  data := NewLapData(`[`)//, p.Parent.Name, ``)
  p.copy_out_fields(&p.last,&data)
  data.AddNumber(p.Group_field,p.elements[0].index)

  //LapLog("AGGR SEND0 %+v out=%v",p.elements[0],p.Out_field)
  r:=p.elements[0].res
  if p.aggregation_type==aggr_avg {
    r=r/float64(p.elements[0].count)
  }
  data.AddNumber(p.Out_field,r)
  p.Parent.SendToAll(&data)
  LapLog("AGGR SEND: %+v r=%v",data,r)
  p.elements=p.elements[1:]
}
func (p LapAggregateCount) StartWork(*LapNode, *LapData, string) {
}

func (p *LapAggregateCount) Input(node *LapNode, d *LapData, src string) {
  p.Parent=node
  //LapLog("AGGD: GOT %v",d.Cmd)
  if d.Cmd == `x` {
    // EOD

    LapLog("AGGR: EOD!")
    for len(p.elements)>0 {
      p.send_n_del0()
    }
  }else{
    n,_:=d.GetNum(p.Count_field)
    grp,_:=d.GetNum(p.Group_field)
    i := sort.Search(len(p.elements),func(j int) bool{
          //LapLog("... %v",p.elements[j])
          if(p.elements[j]==nil){return false}
          return grp==p.elements[j].index
       })
    //LapLog("i=%v n=%v/%v grp=%v/%v",i,p.Count_field,n,p.Group_field,grp)
    if i>=len(p.elements) {
      //LapLog("NOT FOUND")
      // NOT found
      // create new
      new_data:=new(LapAggregateCountOne)
      new_data.count=1
      new_data.is_first=false
      new_data.res=n
      new_data.index=grp
      //new_data.aggregation_type=p.aggregation_type

      if int64(len(p.elements))>=p.MaxBuf {
        // send oldest data and delete it
        p.send_n_del0()
      }
      p.elements=append(p.elements,new_data)
      LapLog("AGGR: NEW: n=%v, grp=%v, %+v",n,grp,new_data)
    }else{
      LapLog("AGGR FOUND! n=%v, grp=%v, %+v",n,grp,p.elements[i])
      if p.elements[i].is_first {
        p.elements[i].res=n
        p.elements[i].is_first=false
      }else{
        switch p.aggregation_type {
        case aggr_min:
          if p.elements[i].res>n { p.elements[i].res=n }
        case aggr_max:
          if p.elements[i].res<n { p.elements[i].res=n }
        case aggr_sum, aggr_avg:
          p.elements[i].res+=n
        }
      }
      p.elements[i].count+=1
    }
    p.last=*d
  }
}

func (p *LapAggregateCount) NewProcessor(d LapData) LapDataProcessor{
  a:=new(LapAggregateCount)
  a.Count_field,_=d.GetStr(`count_field`)
  a.Out_field,_=d.GetStr(`out_field`)
  a.Group_field,_=d.GetStr(`grp_field`)
  a.fill_out_fields(d)
  a.elements=make([]*LapAggregateCountOne,0)

  t,_:=d.GetStr(`agr_type`)
  switch t {
  case `min`:
    a.aggregation_type=aggr_min
  case `max`:
    a.aggregation_type=aggr_max
  case `sum`:
    a.aggregation_type=aggr_sum
  case `avg`:
    a.aggregation_type=aggr_avg
  default:
    panic("NO TYPE FOR AGGREGATION")
  }
  buf,ok:=d.GetStr(`maxbuf`)
  if ok {
    i64,_:=strconv.ParseInt(buf,0,0)
    a.MaxBuf=i64
  }else{
    a.MaxBuf=32
  }

//  LapLog("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  return a
}
func (p *LapAggregateCount) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}


func (p LapAggregateCount) GetSize() string {
  return fmt.Sprintf("data=%d",len(p.elements))
}
