package lap_processor

import (
//  "log"
//  "sort"
  "strings"
  "strconv"
//  "strings"
//  "math"
  "fmt"
  "bytes"
  "../helpers"
)

//import "github.com/yasushi-saito/fifo_queue"
//type LapDataMap map[string]LapData

//////////////////////////////////////////////////////////////////////
//    
// LapDataProcessor implementation for grid aggregation data
//    
//////////////////////////////////////////////////////////////////////

type LapGrid struct {
  LapDataCommon

  max_count   int64
  max_hole    int64
  by_field    string
  step        float64
  cur_start   float64
  
  save_fields []string

  grid_fields []string
  aggregation []byte
//  grid_values []float64

  elements    []map[string]LapData
  counts      []map[string]int64
  //el          fifo_queue.Queue
  //is_started  []map[string]bool

// ===> elements/counts index is counted from by_field

  first_received bool
  id        string
}

func (p LapGrid) fillInfo(info *map[string]string) {
  (*info)["type"]="grid"
  (*info)["max_count"]=strconv.FormatInt(p.max_count,10)
  (*info)["max_hole"]=strconv.FormatInt(p.max_hole,10)
  (*info)["by_field"]=p.by_field
  (*info)["step"]=strconv.FormatFloat(p.step,'g',-1,64)
  (*info)["cur_start"]=strconv.FormatFloat(p.cur_start,'g',-1,64)
  (*info)["save_fields"]=strings.Join(p.save_fields,",")
  (*info)["grid_fields"]=strings.Join(p.grid_fields,",")
  (*info)["save_fields"]=strings.Join(p.save_fields,",")
  (*info)["aggregations"]=helpers.Reduce(p.aggregation,"",
    func(mem interface{},val interface{}) interface{} {
        var str string
        switch val.(byte) {
        case aggr_avg:
          str="avg"
        case aggr_min:
          str="min"
        case aggr_max:
          str="max"
        case aggr_sum:
          str="sum"
        default:
          str="unknown"
        }
        if mem.(string) == "" {
          return str
        }
        return mem.(string)+","+str
      }).(string)
}

func (p LapGrid) GetSize() string {
  str:=len(p.save_fields)+len(p.grid_fields)+len(p.aggregation)
  data:=0
  for _,e:=range(p.elements){
    data+=len(e)
  }
  ints:=0
  for _,i:=range(p.counts){
    ints+=len(i)
  }
  return fmt.Sprintf("str=%d, data=%d(%d), ints=%d",str,data,len(p.elements),ints)
}

func (p *LapGrid) logcounts(){
  return
  //for i,val:=range(p.counts){
  for i,val:=range(p.elements){
    // for name,c:=range(val){
    //   LapLog("GRID counts [%d] (%s) = %d",i,name,c)
    // }
    for name,el:=range(val){
      if x,ok:=p.counts[i][name]; ok {
        LapLog("[%s] !! VAL/COUNT: [%d]{%s} = %s/%d",p.Parent.Name,i,name,el,x)
      }else{
        LapLog("[%s] ~~ VAL/COUNT: [%d]{%s} = %s/##",p.Parent.Name,i,name,el)
      }
    }
  }
}

var shift_count int64

func (p *LapGrid) send_n_del0() {
  var field_value float64

  //p.logcounts()
  last:=p.max_count-1
  for name,el:=range(p.elements[last]){
//    LapLog("GRID ---> '%s'",name)
    if _,ok:=p.elements[last][name];ok {
      // Update Average aggregations...
      l:=len(p.grid_fields)
      for a:=0; a<l; a++ {
        if(p.aggregation[a]==aggr_avg){
          val,_:=el.GetNum(p.grid_fields[a])
//          LapLog("GRID AVG: a=%d, f=%v, val=%f count=%f name=%s",a,p.grid_fields[a],val,float64(p.counts[0][name]),name)
          el.AddNumber(p.grid_fields[a],val/float64(p.counts[last][name]))
        }
      }
    }else{
      LapLog("[%s] GRID ALERT: NO VALUE!!!",p.Parent.Name)
      continue
    }
    //LapLog("[%s] GRID SEND: (%s) %+v",p.Parent.Name,name,el)
    p.Parent.SendToAll(&el)
  }

  for name,el:=range(p.elements[last]){
    _,ok:=p.elements[last-1][name]
    if ! ok {
  //    LapLog("GRID translate data: %s %+v",name,p.elements[0])
      p.elements[last-1][name]=*el.Copy()
      // fix 'by_field' value
      dd:=p.elements[last-1][name]
      dd.AddNumber(p.by_field,float64(int64(p.cur_start/p.step)+1)*p.step)
      p.counts[last-1][name]=1
    //  LapLog("GRID translated to: %+v",p.elements[1][name])
      if false {
      shift_count+=1
        if shift_count>1000000 {
          shift_count=0
          LapLog("SHIFT!")
        }
      }
    }

    for _,el:=range(p.elements[last-1]){
      field_value,_=el.GetNum(p.by_field)
      break
    }
  }
  p.cur_start=float64(int64(field_value/p.step))*p.step

  // do reverse shift!
  for i:=last;i>0;i-=1 {
    p.elements[i]=p.elements[i-1]
    p.counts[i]=p.counts[i-1]
  }
  p.counts[0]=make(map[string]int64)
  p.elements[0]=make(map[string]LapData)
  // x:=make([]map[string]LapData,1)
  // x[0]=make(map[string]LapData)
  // if(int64(len(p.elements))!=last+1){
  //   LapLog("Oppps0: %d!=%d",len(p.elements),last+1)
  // }
  // p.elements=append(x,p.elements[0:last-1]...)
  // p.counts=append([]map[string]int64{make(map[string]int64)},p.counts[0:last-1]...)
  // if(int64(len(p.elements))!=last+1){
  //   LapLog("Oppps: %d!=%d",len(p.elements),last+1)
  //   panic("oops")
  // }
}

func (p *LapGrid) send_all() {
  LapLog("[%s] GRID: send_all",p.Parent.Name)
  length:=len(p.elements)
  count:=length
  for i := 0; i < length; i++ {
    if len(p.elements[i])==0 {count-=1}
  }
  for i := 0; i < count; i++ {
    p.send_n_del0()
  }
}


var lap_grid_counter=0

func (p *LapGrid) add(index int64, key string, data *LapData) {
  //p.elements[0]=make(map[string]LapData)
  //p.counts[0]=make(map[string]int64)

  // REVERSED array!
  //LapLog("ADD: max=%d, index=%d, key=%s",p.max_count,index,key)
  p.elements[p.max_count-index-1][key]=*data
  p.counts[p.max_count-index-1][key]=1
  //LapLog("ok")
}
func (p LapGrid) StartWork(*LapNode, *LapData, string) {
}

func (p *LapGrid) Input(n *LapNode, d *LapData, src string) {
  //p.Parent=n
  //LapLog("AGGD: GOT %v",d.Cmd)
  if d.Cmd == `x` {
    // EOD

    LapLog("[%s] GRID: EOD!",p.Parent.Name)
    if p.first_received {
      p.send_all()
    }
    data:=NewLapData(`x`)//,``,``)
    p.Parent.SendToAll(&data)
  }else{
    //d.From=p.Parent.Name
    var buf bytes.Buffer

    for _,name:=range(p.save_fields) {
//      LapLog("GRID: '%s' (%+v)",name,d)
      v:=d.ToStr(name)
      buf.WriteString(" ")
      buf.WriteString(v)
    }
    save_fields_value:=buf.String()

    got_value:=d.ToNum(p.by_field)
    //data_by:=got_value-p.cur_start
    //index_float:=math.Trunc((got_value-p.cur_start)/p.step)
    //index:=int64(index_float)
    index:=int64((got_value-p.cur_start)/p.step)
    aligned_field:=float64(index)*p.step+p.cur_start
    //if p.cur_start<0 {p.cur_start}
    //i:=index //-p.cur_index

    el_index:=p.max_count-1-index
    if ! p.first_received {
      p.first_received=true
      var new_d LapData
      new_d=*d.Copy()

      p.cur_start=aligned_field
      new_d.AddNumber(p.by_field,aligned_field)
      p.add(0,save_fields_value,&new_d)
      
      //LapLog("[%s] GRID FIRST added start=%f index=%d step=%f save_f=%s %+v",p.Parent.Name,p.cur_start,index,p.step,save_fields_value,new_d)
      return
    }

    if index<0 {
      LapLog("[%s] GRID Data is too late. Drop it. (value=%f, step=%f, i=%d) %v",p.Parent.Name,got_value,p.step,index,d)
      return
    }

    if index>p.max_count*p.max_hole{
      // full buffer reset needed...
      p.send_all()
      max:=int(p.max_count)
      for i := 0; i < max; i++ {
        p.elements[i]=make(map[string]LapData)
        p.counts[i]=make(map[string]int64)
      }
      var new_d LapData
      new_d=*d//*d.Copy()
      p.cur_start=aligned_field
      new_d.AddNumber(p.by_field,aligned_field)
      p.add(0,save_fields_value,&new_d)
      return
    }else{
      for index>=p.max_count {
  //      LapLog("SHIFT: Index=%d>=%d (value=%f/%f)",index,p.max_count,got_value,aligned_field)
        p.send_n_del0()
        index-=1
        el_index+=1
      }
    }

    if _,ok:=p.elements[el_index][save_fields_value]; ok {
      // each field aggregation...
      l:=len(p.grid_fields)
//      LapLog("LEN=%d i=%v",l,index)
      for a:=0; a<l; a++ {
        val,ok_new:=d.GetNum(p.grid_fields[a])
//        LapLog("A=%v, VA=%v (%+v), i=%v",a,val,p.grid_fields,index)
        elem:=p.elements[el_index][save_fields_value]
        old,ok_old:=elem.GetNum(p.grid_fields[a])
        if ok_new && ok_old {
          switch p.aggregation[a]{
          case aggr_min:
            if(val<old){
              elem.AddNumber(p.grid_fields[a],val)
            }
          case aggr_max:
            if(val>old){
              elem.AddNumber(p.grid_fields[a],val)
            }
          case aggr_avg:
            elem.AddNumber(p.grid_fields[a],old+val)
          }
        }else{
          elem.AddNumber(p.grid_fields[a],val)
        }
      }
      p.counts[el_index][save_fields_value]+=1
    }else{
      // No value yet...
      var new_d LapData
      new_d=*d.Copy()
      new_d.AddNumber(p.by_field,aligned_field)
//      LapLog("GRID added index=%d [%v=%v] %+v (save=%s)",index,p.by_field,aligned_field,new_d,save_fields_value)
      p.add(index,save_fields_value,&new_d)
      //!p.elements[index][save_fields_value]=new_d
      //!p.counts[index][save_fields_value]=1
      //value:=math.Floor(new_d.GetNum(p.by_field)-p.start)/p.step)
      //p.cur_start=p.start+value*p.step
    }
    // LapLog("New data aggregated. Index=%d/%s data=[%v], count=%v",
    //   index,save_fields_value,p.elements[index][save_fields_value],
    //   p.counts[index][save_fields_value])
    //p.logcounts()
    // lap_grid_counter+=1
    // if lap_grid_counter>1000000{
    //   lap_grid_counter=0
    //   LapLog("GRID: counter...")
    // }
  }
}

func (p *LapGrid)NewProcessor(d LapData) LapDataProcessor{
  a:=new(LapGrid)

  a.by_field,_=d.GetStr(`by_field`)
  a.max_count=int64(d.ToNum(`max_count`))
  if a.max_count<1 {a.max_count=32}
  num:=d.ToNumDef(`max_hole`,0)
  a.max_hole=int64(num)
  if a.max_hole<2 {a.max_hole=2}
  a.cur_start=d.ToNum(`start`)
  a.step=d.ToNum(`step`)
  if a.step==0.0 {a.step=1; LapLog("STEP set to 1")}

  a.fill_out_fields(d)
  a.elements=make([]map[string]LapData,a.max_count)
  a.counts=make([]map[string]int64,a.max_count)
  //a.is_started  =make([]map[string]bool,a.max_count)
  //a.grid_values =make([]float64,fields_count)

  fstr,_:=d.GetStr(`grid_fields`)
  fields:=strings.Split(fstr,",")
  fields_count:=len(fields)
  //LapLog("+++gf=%s, ga=%s",fstr,fields)

  a.grid_fields =make([]string,fields_count)
  a.aggregation =make([]byte,fields_count)

  // FORMAT: name[/aggregation[=default]],...
  for i,fld := range fields {
    s:=strings.Split(fld,"/")
    a.grid_fields[i]=s[0]
    if len(s)==1 {
      a.aggregation[i]=aggr_min
      //a.grid_defs[i]=0
    }else{
      d:=strings.Split(s[1],"=")
      switch d[0]{
      case "min":
        a.aggregation[i]=aggr_min
      case "max":
        a.aggregation[i]=aggr_max
      case "avg":
        a.aggregation[i]=aggr_avg
      default:
        a.aggregation[i]=aggr_min
      }
      // if len(d)==1 {
      //   a.grid_defs[i]=0
      // }else{
      //   a.grid_defs[i],_=strconv.ParseFloat(d[1],64)
      // }
    }
  }

  max:=int(a.max_count)
  for i := 0; i < max; i++ {
    //a.is_started[i]=make(map[string]bool)
    a.elements[i]=make(map[string]LapData)
    a.counts[i]=make(map[string]int64)
  }
  //a.is_started[0]=false
  // var v LapData
  // v=NewLapData("","","")
  // a.elements[0]=&v
  // for i,fld := range a.grid_fields {
  //   a.elements[0].AddNumber(fld,a.grid_defs[i])
  // }
  a.first_received=false

  sf,_:=d.GetStr(`save_fields`)
  a.save_fields=strings.Split(sf,`,`)
  LapLog("+++s=%s, a=%s",sf,a.save_fields)
  if sf==`` {
    LapLog("ERROR!!!! No save_fields!")
  }

  LapLog("New GRID: %+v",*a)
  return a
}
func (p *LapGrid) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
