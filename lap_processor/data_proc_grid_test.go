package lap_processor

import (
  "testing"
)

// type time_val struct{
//   time float64
//   val float64
// }

//
//  Buffered "true" average count
//  All values are stored in sorted buffer (by time),
//  result is counted as SUM( VAL[i]*(T[i]-T[i-1]) ) / Interval_length
//  if avg_to_end is used, formulae is
//  SUM( VAL[i]*(T[i+1]-T[i]) ) / Interval_length
//
// type LapAvgBuff struct {
//   values []time_val
//   max_count int
//   kind      int

//   current_sum float64
//   interval    float64
//   last_time   float64
//   start_time  float64
// }

//const max_count int = 3
const interval float64 = 50.0
const base_time float64 = 0.0

func TestLapAvgBuff_all(t *testing.T) {
  kinds := []struct {
    i   int
    s   string
    sum float64
  }{
    {aggr_avg_true, "end", 18.0},
    {aggr_avg_true_start, "start", 17.0},
  }
  values := [][2]float64{{10.0, 10.0}, {35.0, 20.0}, {20.0, 40.0}, {40.0, 10.0}}
  counts := []int{1, 2, 3, 10}

  t.SkipNow()
  for _, count := range counts {
    for _, kind := range kinds {
      t.Logf("Kind: %s, max_count: %d\n", kind.s, count)
      ab := NewLapAvgBuff(count, kind.i, interval, base_time)
      for _, val := range values {
        ab.add(val[0], val[1])
      }
      result := ab.read()
      if result != kind.sum {
        t.Fail() //"Kind: %s, max_count: %d, expected: %f, got %f\n", kind.s,count,kind.sum,result)
      }
      t.Logf("expected: %f, got: %f\n", kind.sum, result)
    }
  }
}

func TestLapAvgBuff_all2(t *testing.T) {
  kinds := []struct {
    i   int
    s   string
    sum float64
  }{
    {aggr_avg_true, "end", 114.0},
    {aggr_avg_true_start, "start", 97.2},
  }
  values := [][2]float64{{20.0, 121.0}, {40.0, 122.0}, {0.0, 103.0}}
  counts := []int{128}

  //t.SkipNow()
  for _, count := range counts {
    for _, kind := range kinds {
      t.Logf("Kind: %s (%d), max_count: %d\n", kind.s, kind.i, count)
      ab := NewLapAvgBuff(count, kind.i, interval, base_time)
      for _, val := range values {
        ab.add(val[0], val[1])
      }
      result := ab.read()
      if result != kind.sum {
        t.Fail() //"Kind: %s, max_count: %d, expected: %f, got %f\n", kind.s,count,kind.sum,result)
      }
      t.Logf("expected: %f, got: %f\n", kind.sum, result)
    }
  }
  //t.FailNow()
}

func TestLapAvgData_one(t *testing.T) {
  kinds := []struct {
    i                int
    s                string
    sum1, sum2, sum3 float64
  }{
    {aggr_min, "min", 0.0, 22.0, 103.0},
    {aggr_max, "max", 0.0, 22.0, 122.0},
    {aggr_avg, "avg", 0.0, 22.0, (103.0 + 121.0 + 122.0) / 3},
    {aggr_sum, "sum", 0.0, 22.0, 346.0},
    {aggr_avg_true, "end", 0.0, 22.0, 114.0},
    {aggr_avg_true_start, "start", 0.0, 22.0, 97.2},
  }
  values := []struct {
    str  string
    val  float64
    time float64
  }{
    {"var3", 121.0, 20.0},
    {"var3", 122.0, 40.0},
  }

  //t.SkipNow()
  for _, kind := range kinds {
    init_data := NewLapData(`x`, `var1`, 0.0, `var2`, 22.0, `var3`, 103.0)
    t.Logf("Kind: %s (%d), init_data=%+v\n", kind.s, kind.i, init_data)
    ab := NewLapAggData(kind.i, base_time, interval, init_data)
    for _, val := range values {
      ab.Put(val.str, val.val, val.time)
    }
    result := ab.Get()
    t.Logf("Got: %+v (expect %f)", result, kind.sum3)
    if result.ToNum(`var3`) != kind.sum3 {
      t.Fail() //"Kind: %s, max_count: %d, expected: %f, got %f\n", kind.s,count,kind.sum,result)
    }
    //t.Logf("expected: %f, got: %f\n",kind.sum,result)
  }
}

func TestLapAvgData_all(t *testing.T) {
  kinds := []struct {
    i                int
    s                string
    sum1, sum2, sum3 float64
  }{
    {aggr_min, "min", 18.0, 0.0, 0.0},
    {aggr_sum, "sum", 18.0, 0.0, 0.0},
    {aggr_max, "max", 17.0, 0.0, 0.0},
    {aggr_avg_true, "end", 17.0, 0.0, 0.0},
    {aggr_avg_true_start, "start", 17.0, 0.0, 0.0},
  }
  values := []struct {
    str  string
    val  float64
    time float64
  }{
    {"var1", 11.0, 10.0}, // v1 11,12,13,14,15
    {"var1", 12.0, 10.0}, // v2 21,22,23,24,25
    {"var2", 24.0, 10.0}, // v3 121,122
    {"var1", 14.0, 10.0},
    {"var2", 23.0, 10.0},
    {"var1", 15.0, 10.0},

    {"var2", 21.0, 20.0},
    {"var1", 13.0, 20.0},
    {"var3", 121.0, 20.0},
    {"var2", 25.0, 20.0},

    {"var3", 122.0, 40.0},
    {"var2", 22.0, 40.0},
  }
  return

  for _, kind := range kinds {
    init_data := NewLapData(`x`, `var1`, 0.0, `var2`, 22.0, `var3`, 103.0)
    t.Logf("Kind: %s, init_data=%+v\n", kind.s, init_data)
    ab := NewLapAggData(kind.i, base_time, interval, init_data)
    for _, val := range values {
      ab.Put(val.str, val.val, val.time)
    }
    result := ab.Get()
    t.Logf("Got: %+v", result)
    if result.ToNum(`var1`) != kind.sum1 {
      t.Fail() //"Kind: %s, max_count: %d, expected: %f, got %f\n", kind.s,count,kind.sum,result)
    }
    if result.ToNum(`var2`) != kind.sum2 {
      t.Fail() //"Kind: %s, max_count: %d, expected: %f, got %f\n", kind.s,count,kind.sum,result)
    }
    if result.ToNum(`var3`) != kind.sum3 {
      t.Fail() //"Kind: %s, max_count: %d, expected: %f, got %f\n", kind.s,count,kind.sum,result)
    }
    //t.Logf("expected: %f, got: %f\n",kind.sum,result)
  }
}

// type LapAggData struct {
//   data LapData
//   buf  map[string]LapAvgBuff
// //  is_data bool
//   data_type int
//   counter int64
//   start_time float64
//   interval   float64
// }

// func NewLapAggData(kind int, start_time float64, interval float64, data LapData) LapAggData {
//   ret:=LapAggData{data_type:kind, counter:0, data:data, start_time:start_time, interval:interval}
//   ret.buf=make(map[string]LapAvgBuff,0)
//   return ret
// }

// func (p *LapAggData) GetNum(s string) float64 {
//   switch p.data_type {
//   case aggr_min,aggr_max,aggr_sum:
//     return p.data.ToNum(s)
//   case aggr_avg:
//     return p.data.ToNum(s)/float64(p.counter)
//   case aggr_avg_true:
//     x:=p.buf[s]
//     return (&x).read()
//   default:
//     LapLog("Bad aggregation type. Internal error 101.")
//   }
//   return -404.0
// }

// func (p *LapAggData) Get() LapData {
//   switch p.data_type {
//   case aggr_min,aggr_max,aggr_sum:
//     return p.data
//   case aggr_avg:
//     return p.data //!!!  AVG
//   case aggr_avg_true:
//     for name,value:=range p.buf{
//       p.data.AddNumber(name,value.read())
//     }
//     return p.data
//   default:
//     LapLog("Bad aggregation type. Internal error 101.")
//   }
//   return NewLapData(`x`)
// }

// const BUF_MAX_COUNT int = 128
// const BUF_AVG_KIND  int = avg_from_start

// func (p *LapAggData) Put(s string, n float64, time float64) {
//   switch p.data_type {
//   case aggr_min:
//     last:=p.data.ToNum(s)
//     if(last>n){last=n}
//     p.data.AddNumber(s,last)
//   case aggr_max:
//     last:=p.data.ToNum(s)
//     if(last<n){last=n}
//     p.data.AddNumber(s,last)
//   case aggr_sum,aggr_avg:
//     last:=p.data.ToNum(s)
//     last+=n
//     p.data.AddNumber(s,last)
//     p.counter+=1
//   case aggr_avg_true:
//     if _,ok:=p.buf[s];!ok {
//       p.buf[s]=NewLapAvgBuff(BUF_MAX_COUNT,BUF_AVG_KIND,p.interval,p.start_time)
//     }
//     x:=p.buf[s]
//     (&x).add(time,n)
//   }
// }

// //////////////////////////////////////////////////////////////////////
// //
// // LapDataProcessor implementation for grid aggregation data
// //
// //////////////////////////////////////////////////////////////////////

// type LapGrid struct {
//   LapDataCommon

//   max_count   int64
//   max_hole    int64
//   by_field    string
//   step        float64
//   cur_start   float64

//   mult        float64
//   shift       float64
//   do_change   bool

//   save_fields []string

//   grid_fields []string
//   aggregation []byte
// //  grid_values []float64

//   //                  /~--- uniq values of 'save_fields'
//   elements    []map[string]LapAggData
//   counts      []map[string]int64
//   //el          fifo_queue.Queue
//   //is_started  []map[string]bool

// // ===> elements/counts index is counted from by_field

//   first_received bool
//   id        string
// }

// func (p LapGrid) fillInfo(info *map[string]string) {
//   (*info)["type"]="grid"
//   (*info)["max_count"]=strconv.FormatInt(p.max_count,10)
//   (*info)["max_hole"]=strconv.FormatInt(p.max_hole,10)
//   (*info)["by_field"]=p.by_field
//   (*info)["step"]=strconv.FormatFloat(p.step,'g',-1,64)
//   (*info)["cur_start"]=strconv.FormatFloat(p.cur_start,'g',-1,64)
//   (*info)["save_fields"]=strings.Join(p.save_fields,",")
//   (*info)["grid_fields"]=strings.Join(p.grid_fields,",")
//   (*info)["save_fields"]=strings.Join(p.save_fields,",")
//   (*info)["aggregations"]=helpers.Reduce(p.aggregation,"",
//     func(mem interface{},val interface{}) interface{} {
//         var str string
//         switch val.(byte) {
//         case aggr_avg:
//           str="avg"
//         case aggr_avg_true:
//           str="avg_true"
//         case aggr_min:
//           str="min"
//         case aggr_max:
//           str="max"
//         case aggr_sum:
//           str="sum"
//         default:
//           str="unknown"
//         }
//         if mem.(string) == "" {
//           return str
//         }
//         return mem.(string)+","+str
//       }).(string)
// }

// func (p LapGrid) GetSize() string {
//   str:=len(p.save_fields)+len(p.grid_fields)+len(p.aggregation)
//   data:=0
//   for _,e:=range(p.elements){
//     data+=len(e)
//   }
//   ints:=0
//   for _,e:=range(p.counts){
//     ints+=len(e)
//   }
//   return fmt.Sprintf("str=%d, data=%d(%d), ints=%d",str,data,len(p.elements),ints)
// }

// func (p *LapGrid) logcounts(){
//   return
//   //for i,val:=range(p.counts){
//   for i,val:=range(p.elements){
//     // for name,c:=range(val){
//     //   LapLog("GRID counts [%d] (%s) = %d",i,name,c)
//     // }
//     for name,el:=range(val){
//       if x,ok:=p.counts[i][name]; ok {
//         LapLog("[%s] !! VAL/COUNT: [%d]{%s} = %s/%d",p.Parent.Name,i,name,el,x)
//       }else{
//         LapLog("[%s] ~~ VAL/COUNT: [%d]{%s} = %s/##",p.Parent.Name,i,name,el)
//       }
//     }
//   }
// }

// var shift_count int64

// func (p *LapGrid) send_n_del0() {
//   var field_value float64

//   //p.logcounts()

//   // send oldest data
//   last:=p.max_count-1
//   for _,el:=range(p.elements[last]){
//     data:=el.Get()
//     p.Parent.SendToAll(&data)
//   }

//   for name,el:=range(p.elements[last]){
//     _,ok:=p.elements[last-1][name]
//     if ! ok {
//   //    LapLog("GRID translate data: %s %+v",name,p.elements[0])
//       //p.elements[last-1][name]=*el.Copy()
//       // fix 'by_field' value
//       //p.elements[last-1][name].AddNumber(p.by_field,float64(int64(p.cur_start/p.step)+1)*p.step)
//       p.counts[last-1][name]=1

//       p.elements[last-1][name]=NewLapAggData(AGG_TYPE, float64(int64(p.cur_start/p.step)+1)*p.step, p.step, el.Get())

//     //  LapLog("GRID translated to: %+v",p.elements[1][name])
//       if false {
//       shift_count+=1
//         if shift_count>1000000 {
//           shift_count=0
//           LapLog("SHIFT!")
//         }
//       }
//     }

//     for _,el:=range(p.elements[last-1]){
//       field_value,_=el.Get().GetNum(p.by_field)
//       break
//     }
//   }
//   p.cur_start=float64(int64(field_value/p.step))*p.step

//   // do reverse shift!
//   for i:=last;i>0;i-=1 {
//     p.elements[i]=p.elements[i-1]
//     p.counts[i]=p.counts[i-1]
//   }
//   p.counts[0]=make(map[string]int64)
//   p.elements[0]=make(map[string]LapAggData)
// }

// func (p *LapGrid) send_all() {
//   LapLog("[%s] GRID: send_all",p.Parent.Name)
//   length:=len(p.elements)
//   count:=length
//   for i := 0; i < length; i++ {
//     if len(p.elements[i])==0 {count-=1}
//   }
//   for i := 0; i < count; i++ {
//     p.send_n_del0()
//   }
// }

// var lap_grid_counter=0

// // func (p *LapGrid) add(index int64, key string, data LapData) {
// //   //p.elements[0]=make(map[string]LapData)
// //   //p.counts[0]=make(map[string]int64)

// //   // REVERSED array!
// //   //LapLog("ADD: max=%d, index=%d, key=%s",p.max_count,index,key)
// //   p.elements[p.max_count-index-1][key]=data
// //   p.counts[p.max_count-index-1][key]=1
// //   //LapLog("ok")
// // }

// func (p LapGrid) StartWork(*LapNode, *LapData, string) {
// }

// const AGG_TYPE int =aggr_avg

// func (p *LapGrid) Input(n *LapNode, d *LapData, src string) {
//   //p.Parent=n
//   //LapLog("AGGD: GOT %v",d.Cmd)
//   if d.Cmd == `x` {
//     // EOD

//     LapLog("[%s] GRID: EOD!",p.Parent.Name)
//     if p.first_received {
//       p.send_all()
//     }
//     data:=NewLapData(`x`)//,``,``)
//     p.Parent.SendToAll(&data)
//   }else{
//     //d.From=p.Parent.Name
//     var buf bytes.Buffer

//     for _,name:=range(p.save_fields) {
// //      LapLog("GRID: '%s' (%+v)",name,d)
//       v:=d.ToStr(name)
//       buf.WriteString(" ")
//       buf.WriteString(v)
//     }
//     save_fields_value:=buf.String()

//     time_value:=d.ToNum(p.by_field)
//     if p.do_change {
//       time_value=math.Trunc(time_value*p.mult)+p.shift
//     }
//     //data_by:=got_value-p.cur_start
//     //index_float:=math.Trunc((got_value-p.cur_start)/p.step)
//     //index:=int64(index_float)
//     index:=int64((time_value-p.cur_start)/p.step)
//     aligned_field:=math.Trunc(float64(index)*p.step+p.cur_start)
//     //if p.cur_start<0 {p.cur_start}
//     //i:=index //-p.cur_index

//     el_index:=p.max_count-1-index
//     if ! p.first_received {
//       p.first_received=true
//       var new_d LapData
//       new_d=*d.Copy()

//       p.cur_start=aligned_field
//       //new_d.AddNumber(p.by_field,aligned_field)
//       //p.add(0,save_fields_value,&new_d)

//       p.elements[p.max_count-1][save_fields_value]=NewLapAggData(AGG_TYPE, aligned_field, p.step, new_d)

//       //LapLog("[%s] GRID FIRST added start=%f index=%d step=%f save_f=%s %+v",p.Parent.Name,p.cur_start,index,p.step,save_fields_value,new_d)
//       return
//     }

//     if index<0 {
//       LapLog("[%s] GRID Data is too late. Drop it. (value=%f, step=%f, i=%d) %v",p.Parent.Name,time_value,p.step,index,d)
//       return
//     }

//     if index>p.max_count*p.max_hole{
//       // full buffer reset needed...
//       LapLog("[%s] GRID HOLE filling...",p.Parent.Name)
//       p.send_all()
//       max:=int(p.max_count)
//       for i := 0; i < max; i++ {
//         p.elements[i]=make(map[string]LapAggData)
//         //p.counts[i]=make(map[string]int64)
//       }
//       var new_d LapData
//       new_d=*d.Copy()
//       p.cur_start=aligned_field
//       //new_d.AddNumber(p.by_field,aligned_field)
//       //p.add(0,save_fields_value,&new_d)
//       p.elements[p.max_count-1][save_fields_value]=NewLapAggData(AGG_TYPE, aligned_field, p.step, new_d)
//       return
//     }else{
//       for index>=p.max_count {
//         p.send_n_del0()
//         index-=1
//         el_index+=1
//       }
//     }

//     if elem,ok:=p.elements[el_index][save_fields_value]; ok {
//       // each field aggregation...
//       //l:=len(p.grid_fields)
// //      LapLog("LEN=%d i=%v",l,index)
//       //for a:=0; a<l; a++ {

//       for _,grid_f:=range p.grid_fields{
//         val,ok_f:=d.GetNum(grid_f)
//         //old,ok_old:=elem.GetNum(p.grid_fields[a])
//         if ok_f {//} && ok_old {
//           elem.Put(grid_f,val,time_value)

//         //   switch p.aggregation[a]{
//         //   case aggr_min:
//         //     if(val<old){
//         //       elem.AddNumber(p.grid_fields[a],val)
//         //     }
//         //   case aggr_max:
//         //     if(val>old){
//         //       elem.AddNumber(p.grid_fields[a],val)
//         //     }
//         //   case aggr_avg:
//         //     elem.AddNumber(p.grid_fields[a],old+val)
//         //   }
//         // }else{
//         //   elem.AddNumber(p.grid_fields[a],val)
//         }
//       }
// //      p.counts[el_index][save_fields_value]+=1
//     }else{
//       // No value yet...
//       var new_d LapData
//       new_d=*d.Copy()
//       //new_d.AddNumber(p.by_field,aligned_field)
// //      LapLog("GRID added index=%d [%v=%v] %+v (save=%s)",index,p.by_field,aligned_field,new_d,save_fields_value)
//       //p.add(index,save_fields_value,&new_d)
//       p.elements[p.max_count-1][save_fields_value]=NewLapAggData(AGG_TYPE, aligned_field, p.step, new_d)
//       //!p.elements[index][save_fields_value]=new_d
//       //!p.counts[index][save_fields_value]=1
//       //value:=math.Floor(new_d.GetNum(p.by_field)-p.start)/p.step)
//       //p.cur_start=p.start+value*p.step
//     }
//     // LapLog("New data aggregated. Index=%d/%s data=[%v], count=%v",
//     //   index,save_fields_value,p.elements[index][save_fields_value],
//     //   p.counts[index][save_fields_value])
//     //p.logcounts()
//     // lap_grid_counter+=1
//     // if lap_grid_counter>1000000{
//     //   lap_grid_counter=0
//     //   LapLog("GRID: counter...")
//     // }
//   }
// }

// func (p *LapGrid) NewProcessor(d LapData) LapDataProcessor{
//   a:=new(LapGrid)

//   a.by_field,_=d.GetStr(`by_field`)
//   a.max_count=int64(d.ToNum(`max_count`))
//   if a.max_count<1 {a.max_count=32}
//   num:=d.ToNumDef(`max_hole`,0)
//   a.max_hole=int64(num)
//   if a.max_hole<2 {a.max_hole=2}
//   a.cur_start=d.ToNum(`start`)
//   a.step=d.ToNum(`step`)
//   if a.step==0.0 {a.step=1; LapLog("STEP set to 1")}

//   a.shift=d.ToNum(`shift`)
//   a.mult=d.ToNumDef(`mult`,1.0)
//   if a.shift!=0.0 && a.mult!=1.0 {
//     a.do_change=true
//   }else{
//     a.do_change=false
//   }

//   a.fill_out_fields(d)
//   a.elements=make([]map[string]LapAggData,a.max_count)
//   a.counts=make([]map[string]int64,a.max_count)
//   //a.is_started  =make([]map[string]bool,a.max_count)
//   //a.grid_values =make([]float64,fields_count)

//   fstr,_:=d.GetStr(`grid_fields`)
//   fields:=strings.Split(fstr,",")
//   fields_count:=len(fields)
//   //LapLog("+++gf=%s, ga=%s",fstr,fields)

//   a.grid_fields =make([]string,fields_count)
//   a.aggregation =make([]byte,fields_count)

//   // FORMAT: name[/aggregation[=default]],...
//   for i,fld := range fields {
//     s:=strings.Split(fld,"/")
//     a.grid_fields[i]=s[0]
//     if len(s)==1 {
//       a.aggregation[i]=aggr_min
//       //a.grid_defs[i]=0
//     }else{
//       d:=strings.Split(s[1],"=")
//       switch d[0]{
//       case "min":
//         a.aggregation[i]=aggr_min
//       case "max":
//         a.aggregation[i]=aggr_max
//       case "avg":
//         a.aggregation[i]=aggr_avg
//       case "avg_true":
//         a.aggregation[i]=aggr_avg_true
//       default:
//         a.aggregation[i]=aggr_min
//       }
//       // if len(d)==1 {
//       //   a.grid_defs[i]=0
//       // }else{
//       //   a.grid_defs[i],_=strconv.ParseFloat(d[1],64)
//       // }
//     }
//   }

//   max:=int(a.max_count)
//   for i := 0; i < max; i++ {
//     //a.is_started[i]=make(map[string]bool)
//     a.elements[i]=make(map[string]LapAggData)
//     a.counts[i]=make(map[string]int64)
//   }
//   //a.is_started[0]=false
//   // var v LapData
//   // v=NewLapData("","","")
//   // a.elements[0]=&v
//   // for i,fld := range a.grid_fields {
//   //   a.elements[0].AddNumber(fld,a.grid_defs[i])
//   // }
//   a.first_received=false

//   sf,_:=d.GetStr(`save_fields`)
//   a.save_fields=strings.Split(sf,`,`)
//   LapLog("+++s=%s, a=%s",sf,a.save_fields)
//   if sf==`` {
//     LapLog("ERROR!!!! No save_fields!")
//   }

//   LapLog("New GRID: %+v",*a)
//   return a
// }
// func (p *LapGrid) SetId(s string){
//   p.id=s
//   LapLog("Updated id: %v",p)
// }
