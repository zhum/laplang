package lap_processor

import (
//  "log"
  "os"
  "io"
  "io/ioutil"
  "bufio"
  "strings"
  "strconv"
  "../helpers"
//  "bytes"
)
//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor for reading file
//    
//////////////////////////////////////////////////////////////////////
type LapCSVRead struct {
  LapDataCommon

  fd        *os.File
  field_types map[string]int8
  id        string
}

func (p LapCSVRead) StartWork(*LapNode, *LapData, string) {
}

func (p LapCSVRead) GetSize() string {
  return "const"
}

func (p LapCSVRead) fillInfo(info *map[string]string) {
  (*info)["type"]="csv_read"
}

func (p *LapCSVRead) ReadFile() {
  var data LapData
  var err error
  var num float64

  defer p.fd.Close()

  //LapLog("FILE READ")
  r := bufio.NewReader(p.fd)
  bytes, _, err:= r.ReadLine()
  line:=string(bytes[:])
  for err == nil {
    fields:=strings.Split(line,`;`)
    if len(fields)<len(p.OutFields) {
      LapLog("FILE ERROR: bad string (%s); skip",line)
      continue
    }
    //LapLog("FILE READ out=%v fields: %v",p.OutFields,fields)
    data = NewLapData(`[`)//, p.Parent.Name, ``)
    for i,f:=range(p.OutFields){
      if p.field_types[f]==LapNum {
        num,err=strconv.ParseFloat(fields[i],64)
        data.AddNumber(f,num)
      }else{
        data.AddString(f,strings.TrimSpace(fields[i])) //FIXME: remove trimming?
      }
    }
//    LapLog("[%s] Sending '%+v'",p.Parent.Name,data)
    p.Parent.SendToAll(&data)

    bytes, _, err = r.ReadLine()
    line=string(bytes[:])
  }
  if err != io.EOF {
    LapLog("Error while file reading: %s",err)
    return
  }
}

func (p *LapCSVRead) Input(n *LapNode, d *LapData, src string) {
  p.Parent=n

  if d.Cmd == `x` {
    p.Parent.SendToAll(d)
    return
  }

  go p.ReadFile()
  LapLog("Read Thread started")
}

func (p *LapCSVRead) NewProcessor(d LapData) LapDataProcessor{
  a:=LapCSVRead{}
  a.field_types=make(map[string]int8)

  LapLog("NEW FILE: %s",StrAny(``,` `,d))
  //a.fill_out_fields(d)


  fname,_:=d.GetStr(`filename`)
  LapLog("NEW FILE fname=%s",fname)
  if fname!= `` {
    a.fd,_=os.Open(fname)
  }
  fname,_=d.GetStr(`headers`)
  if fname!= `` {
    h,_:=ioutil.ReadFile(fname)
    line:=strings.TrimSpace(string(h))
    LapLog("[FILE] headers line: %s",line)
    //line:=string(h[:])
    // i:=strings.IndexAny(line,"\n\r")
    // if i>=0 {
    //   line=line[0:i]
    // }
    for _,field:=range(strings.Split(line,`;`)){
      if(len(field)<3){continue}
      //LapLog("...... head: %s (%s)",field,field[1:2])
      if field[1:2]==`:` {
        name:=strings.TrimSpace(field[2:])
        LapLog("[FILE] CSV header name: %s",name)
        a.OutFields=append(a.OutFields,name)
        if field[0:1]==`n` {
          a.field_types[name]=LapNum
        }else{
          a.field_types[name]=LapStr
        }
      }
    }
  }else{
    LapLog("No header file!")
    panic("ooops")
  }
  return &a
}
func (p *LapCSVRead) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}


///////////////////////////////////////////////////////////////////
//
//  Data processor: print data as csv
//
///////////////////////////////////////////////////////////////////
type LapCSVOut struct {
  LapDataCommon
  f *os.File
  fields []string
  head_print bool
  print_source bool
  lines_count int64
  lines_count_max int64
}

func (p LapCSVOut) StartWork(*LapNode, *LapData, string) {
}

func (p *LapCSVOut) SetId(s string) {
}

func (p LapCSVOut) GetSize() string {
  return "const"
}

func (p LapCSVOut) fillInfo(info *map[string]string) {
  (*info)["type"]="csv_out"
  (*info)["fields"]=helpers.Reduce(p.fields,"",
    func(mem interface{},val interface{}) (interface{}) {
      if mem.(string) == "" {
        return val.(string)
      }
      return mem.(string)+","+val.(string)
    }).(string)
  (*info)["print_header"]=strconv.FormatBool(p.head_print)
  (*info)["print_source"]=strconv.FormatBool(p.print_source)
  (*info)["lines_count_max"]=strconv.FormatInt(p.lines_count_max,10)
  (*info)["lines_count"]=strconv.FormatInt(p.lines_count,10)
}

func (p *LapCSVOut) print_header(){
  not_first:=false
  for _,fname:=range(p.fields){
    if not_first {p.f.WriteString(`,`)}
    not_first=true
    p.f.WriteString(fname)
  }
  if(p.print_source){
    p.f.WriteString(`,source`)
  }
  p.f.WriteString("\n")
}

func (p *LapCSVOut) Input(n *LapNode, d *LapData, src string) {
  p.Parent=n
  if d.Cmd == `[` {
//    LapLog("GOT DATA %+v",d)
    if len(p.fields)<1 {
      // fill fields names
      for name,_:=range(d.NumFields){
        p.fields=append(p.fields,name)
      }
      for name,_:=range(d.StrFields){
        p.fields=append(p.fields,name)
      }
      if(p.head_print){p.print_header()}
    }
    not_first:=false
    for _,fname:=range(p.fields){
      if not_first {p.f.WriteString(`,`)}
      not_first=true
      p.f.WriteString(d.ToStr(fname))
    }
    // if(p.print_source){
    //   p.f.WriteString(`,`)
    //   p.f.WriteString(d.From)
    // }
    p.f.WriteString("\n")
    p.lines_count+=1
    if(p.lines_count>p.lines_count_max){
      p.f.Sync()
      p.lines_count=0
    }
  }else if d.Cmd == `x` {
    p.f.Sync()
    p.lines_count=0
  }else{
    LapLog("[%s]*** COMMAND:%v",p.Parent.Name,d.Cmd);//StrAny(``,` `,d.Fields))  
  }
}

func (p *LapCSVOut) FinishWork(*LapNode, *LapData, string) {
  if p.f != nil {
    p.f.Close()
  }
}


func (p LapCSVOut) NewProcessor(d LapData) LapDataProcessor{
  name,_:=d.GetStr(`filename`)
  ret:=LapCSVOut{}
  ret.fields=make([]string,0)
  ret.lines_count=0
  ret.lines_count_max=1

  ret.f,_=os.OpenFile(name,os.O_WRONLY|os.O_CREATE|os.O_APPEND,0644)

  fields_string,ok:=d.GetStr(`out_fields`)
  if ok {
    ret.fields=strings.Split(fields_string,",")
  }
  ps,_:=d.GetStr(`print_source`)
  if ps == "y" || ps == "1" {
    //LapLog("PRINT HEADERS!!!")
    ret.print_source=true
  }else{
    ret.print_source=false
  }
  ph:=d.ToStr(`print_header`)
  //LapLog("PH=%s",ph)
  if ph == "y" || ph == "1" {
    //LapLog("PRINT HEADERS!!!")
    ret.head_print=true
    if len(ret.fields)>0 {ret.print_header()}
  }else{
    ret.head_print=false
  }
  return &ret
}

///////////////////////////////////////////////////////////////////
//
//  Data processor: print data to /dev/null...
//
///////////////////////////////////////////////////////////////////
type LapNullOut struct {
  LapDataCommon
}

func (p LapNullOut) SetId(s string) {
}

func (p *LapNullOut) Input(n *LapNode, d *LapData, src string) {
  if d.Cmd == `x` {
    LapLog("NULL: got EOD")
  }
}

func (p LapNullOut) fillInfo(info *map[string]string) {
  (*info)["type"]="null_out"
}


func (p LapNullOut) GetSize() string {
  return "const"
}

func (p LapNullOut)NewProcessor(d LapData) LapDataProcessor{
  ret:=LapNullOut{}
  return &ret
}

func (p LapNullOut) StartWork(*LapNode, *LapData, string) {
}

