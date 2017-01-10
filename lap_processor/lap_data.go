package lap_processor

import (
  "log"
  "fmt"
  "runtime"
  "strconv"
//  "sort"
  . "reflect"
  "sync"
)


var (
  id_counter int64 = 0
  node_counter int64 = 0
)

const (
  LapStr = 1 + iota
  LapNum
)

// // One field of LapData
// type LapField struct {
//   Variant int8
//   Str     string
//   Num     float64
// }

//type LapFields []*LapField

// type LapPair struct {
//   S string
//   F *LapField
// }

// func NewLapStr(s string, v string) LapPair{
//   return LapPair{s,&LapField{LapStr,v,0.0}}
// }

// func NewLapNum(s string, v float64) LapPair{
//   return LapPair{s,&LapField{LapNum,``,v}}
// }

// Main type, passed between processors
type LapData struct {
  Cmd    string     // command ('[' for data)
  Id     int64      // almost uniq message ID
//  From   string     // sender
//  To     string     // receiver
  StrFields map[string]string  // string data
  NumFields map[string]float64 // numeric data
}

func NewLapChannel() chan *LapData{
  return make(chan *LapData,1024)//024)
}

func NewLapData(c string, args ... interface{}) LapData{ // f string,t string,
  var field_name string
  p:=LapData{}

  id_counter++
  p.Id=id_counter
  // p.From=f
  // p.To=t
  p.Cmd=c

  field_name=``
  p.StrFields=make(map[string]string)
  p.NumFields=make(map[string]float64)
  for _,a := range args {
    if field_name!=`` {
      switch t:=a.(type){
      case string:
        p.StrFields[field_name]=t
      case float64:
        p.NumFields[field_name]=t
      case int:
        p.NumFields[field_name]=float64(t)
      case int32:
        p.NumFields[field_name]=float64(t)
      case int64:
        p.NumFields[field_name]=float64(t)
      case float32:
        p.NumFields[field_name]=float64(t)
      default:
        LapLog("Bad type in NewLapData (%s) '%+v'",field_name,t)
      }
      field_name=``
    }else{
      switch t:=a.(type){
      case string:
        field_name=t
      // case LapPair:
      //   p.Fields[t.S]=t.F
      default:
        LapLog("Bad type in NewLapData '%+v'",t)
      }
    }
  }
  return p
}

// func NewLapDataPairs(c string,f string,t string, args ... LapPair) LapData{
//   p:=LapData{}

//   id_counter++
//   p.Id=id_counter
//   p.From=f
//   p.To=t
//   p.Cmd=c

//   p.Fields=make(map[string]*LapField)
//   for _,a := range args {
//     p.Fields[a.S]=a.F
//   }
//   return p
// }

func (p LapData) Copy() (*LapData){
  ret:=NewLapData(p.Cmd)//,p.From,p.To)
  for i,v:=range p.NumFields {
    ret.NumFields[i]=v
  }
  for i,v:=range p.StrFields {
    ret.StrFields[i]=v
  }
  return &ret
}

func (p LapData) SetId(n int64) {
  p.Id=n
}

func (p LapData) AddString(name string, s string) {
  p.StrFields[name]=s
}

func (p LapData) AddNumber(name string, n float64) {
  p.NumFields[name]=n
}

// func (p *LapData) AddField(name string, f *LapField) {
//   p.Fields[name]=f
// }

// func (p *LapData) GetField(name string) (*LapField,bool) {
//   ret,ok:=p.Fields[name]
//   return ret,ok
// }

func (p LapData) GetStr(name string) (string,bool) {
  f,ok:=p.StrFields[name]
  if !ok {
    return ``,false
  }
  return f,true
}

func (p LapData) GetNum(name string) (float64,bool){
  f,ok:=p.NumFields[name]
  if !ok {
    return 0.0,false
  }
  return f,true
}

func LapStackWarn(fm string, str ... interface{}){
    stack:=make([]byte,1024)
    runtime.Stack(stack,false)
    LapLog(fm, str...)
    LapLog("STACK: %s",stack) 
}

func (p LapData) CheckField(name string) bool{
  if _,ok:=p.NumFields[name]; ok { return true}
  if _,ok:=p.StrFields[name]; ok { return true}
  return false
}

func (p LapData) ToNum(name string) float64 {
  if v,ok:=p.NumFields[name]; ok{
    return v
  }
  if s,ok2:=p.StrFields[name]; ok2{
    n,_:=strconv.ParseFloat(s,64)
    return n
  }
  LapStackWarn("ERROR in Tonum: No field '%s' in %v",name,p)
  return -404.404
}

func (p LapData) ToNumDef(name string, def float64) float64 {
  if v,ok:=p.NumFields[name]; ok{
    return v
  }
  if s,ok2:=p.StrFields[name]; ok2{
    n,_:=strconv.ParseFloat(s,64)
    return n
  }
  LapStackWarn("ERROR in ToNumDef: No field '%s' in %v",name,p)
  return def
}

func (p LapData) ToStr(name string) string {
  if v,ok:=p.StrFields[name]; ok{
    return v
  }
  if n,ok2:=p.NumFields[name]; ok2{
    return strconv.FormatFloat(n,'f',-1,64)
  }
  LapStackWarn("ERROR in ToStr: No field '%s' in %v",name,p)
  return ``
}

func (p LapData) String() string {
  //ret:="{DATA:"+p.From+"->"+p.To+"'"+p.Cmd+"' "
  ret:="{DATA: '"+p.Cmd+"' N# "
  add_comma:=false
  for k,n:=range(p.NumFields) {
    if add_comma {
      ret+=`, `
    }
    add_comma=true
    ret+=k
    ret+=`:`
    ret+=strconv.FormatFloat(n,'f',-1,64)
  }
  ret+=" S# "
  for s,v:=range(p.StrFields) {
    if add_comma {
      ret+=`, `
    }
    add_comma=true
    ret+=s
    ret+=`:`
    ret+=v
  }
  ret+="}"
  return ret
}

// Process incoming commands
// and call another classes
type LapCmdProcessor interface {
  Input(LapData)
}

var lap_log_mutex = sync.Mutex{}

func LapLog(fm string, str ... interface{}){
  //return
  lap_log_mutex.Lock()
  log.Printf(fm,str...)
  lap_log_mutex.Unlock()
}

var str_any_depth=0
var max_str_any_depth=12

func StrAny(prefix string, indent string, i interface{}) string{
  var a Value
  var ret string

  if str_any_depth>max_str_any_depth {
    return `###`
  }
  str_any_depth+=1
  switch t:=i.(type){
  case Value:
    a=t
  default:
    a=ValueOf(i)
  }
  //LapLog("&&& %+v / %+v",i,a)
  k:=a.Kind().String()

  if k==`interface` {
    a=a.Elem()
    k=a.Kind().String()
  }
  if k==`ptr` {
    a=a.Elem()
    k=a.Kind().String()
  }
  switch k {
  case `nil`:
    ret=`NIL`
  case `chan`:
    ret=`Channel`
  case `int`,`int8`,`int16`,`int32`,`int64`:
    ret=fmt.Sprintf("%s%d",``,a.Int())
  case `float64`,`float32`:
    ret=fmt.Sprintf("%s%f",``,a.Float())
  case `string`:
    ret=fmt.Sprintf("%s'%s'",``,a.String())
  default:
    if k==`struct` {
      ret=fmt.Sprintf("%s{\n%s%s}",``,StrStruct(prefix,indent,a),prefix)
    }else{
      if a.Kind().String()==`array` || a.Kind().String()==`slice` {
        ret=fmt.Sprintf("%s[\n%s%s]",``,StrArray(prefix,indent,a),prefix)
      }else{
        if a.Kind().String()==`map` {
          ret=fmt.Sprintf("%s{\n%s%s}",``,StrMap(prefix,indent,a),prefix)
        }else{
          ret=fmt.Sprintf("~%s~...",k)
        }
      }
    }
  }
  str_any_depth-=1
  return ret
}

func StrStruct(prefix string, indent string, a Value) string{
  num:=a.NumField()
  ret:=``
  str_any_depth+=1
  for i := 0; i < num; i++ {
    f:=a.Field(i)
    //LapLog("::: %+v",f)
    v:=StrAny(prefix+indent,indent,f)
    ret+=fmt.Sprintf("%s%s: %s\n",prefix,a.Type().Field(i).Name,v)
  }
  str_any_depth-=1
  return ret
}

func StrMap(prefix string, indent string, a Value) string{
  keys:=a.MapKeys()
  ret:=``
  str_any_depth+=1
  for _,key:=range(keys) {
    ret+=fmt.Sprintf("%s%s: %s\n",prefix,
      StrAny(prefix+indent,indent,key),
      StrAny(prefix+indent,indent,a.MapIndex(key)))
  }
  str_any_depth-=1
  return ret
}

func StrArray(prefix string, indent string, a Value) string{
  num:=a.Len()
  ret:=``
  str_any_depth+=1
  for i := 0; i < num; i++ {
    v:=StrAny(prefix+indent,indent,a.Index(i))
    ret+=fmt.Sprintf("%s[%d]: %s\n",prefix,i,v)
  }
  str_any_depth-=1
  return ret
}

// for sorting

// func (s LapFields) Len() int      { return len(s) }
// func (s LapFields) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
// func (s LapFields) Less(i, j int) bool {
//   if s[i].Variant==LapNum {
//     if s[j].Variant==LapNum {return s[i].Num<s[j].Num}
//     return strconv.FormatFloat(s[i].Num,'f',-1,64)<s[j].Str
//   }else{
//     if s[i].Variant==LapStr {
//       if s[j].Variant==LapStr{return s[i].Str<s[j].Str}
//     }
//   }
//   return s[i].Str<strconv.FormatFloat(s[i].Num,'f',-1,64)
// }

// func (s *LapField) IsLess(t *LapField) bool {
//   if s.Variant==LapNum {
//     if s.Variant==LapNum {return s.Num<t.Num}
//     return strconv.FormatFloat(s.Num,'f',-1,64)<t.Str
//   }else{
//     if s.Variant==LapStr {
//       if s.Variant==LapStr{return s.Str<t.Str}
//     }
//   }
//   return s.Str<strconv.FormatFloat(s.Num,'f',-1,64)
// }

// func LapFieldInsert (s []*LapField, f *LapField) []*LapField {
//   l:=len(s)
//   if l==0 { ret:=make([]*LapField,1); ret[0]=f; return ret }

//   i := sort.Search(l, func(i int) bool { return s[i].IsLess(f)})
//   if i==l {  // not found = new value is the smallest
//     ret:=make([]*LapField,1)
//     ret[0]=f
//     return append(ret,s...)
//   }
//   if i==l-1 { // new value is the biggest
//     return append(s[0:l],f)
//   }
//   a:=append(s[0:l],f)
//   return append(a,s[l+1:]...)
// }
