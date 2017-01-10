package lap_processor

import (
//  "log"
//  "os"
  "io"
  "net/http"
  "bufio"
  "strings"
  "strconv"
)
//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor for reading http stream
//    
//////////////////////////////////////////////////////////////////////
type LapHttpReadCSV struct {
  LapDataCommon

  res       *http.Response
  field_types map[string]int8
  delim     string
  id        string
}

func (p LapHttpReadCSV) GetSize() string {
  return "const"
}

func (p LapHttpReadCSV) fillInfo(info *map[string]string) {
  (*info)["type"]="read_http"
  (*info)["delim"]=p.delim
}

func (p LapHttpReadCSV) StartWork(*LapNode, *LapData, string) {
}

func (p *LapHttpReadCSV) Input(n *LapNode, d *LapData, src string) {
  var data LapData
  var err error
  var num float64

  if d.Cmd == `x` {
    p.Parent.SendToAll(d)
    return
  }

  //defer p.body.Close()
  if p.res==nil {
    LapLog("No http srteam")
    p.Parent.SendToAll(d)
    return
  }
  LapLog("FILE READ = %+v", p.res.Body)
  r := bufio.NewReader(p.res.Body)
  bytes, _, err:= r.ReadLine()
  line:=string(bytes[:])
  for err == nil {
    fields:=strings.Split(line,p.delim)
    if len(fields)<len(p.OutFields) {
      LapLog("HTTP ERROR: bad string (%s); skip",line)
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
  p.res.Body.Close()
  if err != io.EOF {
    LapLog("Error while http reading: %s",err)
    return
  }
}

func (p *LapHttpReadCSV) NewProcessor(d LapData) LapDataProcessor{
  a:=LapHttpReadCSV{}
  a.field_types=make(map[string]int8)

  //LapLog("NEW FILE: %s",StrAny(``,` `,d))
  //a.fill_out_fields(d)

  dd,ok:=d.GetStr(`delim`)
  if ok {
    p.delim=dd
  }else{
    p.delim=`;`
  }

  fname,_:=d.GetStr(`addr`)
  LapLog("NEW HTTP=%s",fname)
  if fname!= `` {
    //a.fd,_=os.Open(fname)
    resp, err := http.Get(fname)
    if err!=nil {
      LapLog("Cannot open address %s (code=%d)", fname, err)
      //p.body=nil
    }else{
      p.res=resp
      LapLog("HTTP connected: %v",p.res.Body)
    }
  }
  line,_:=d.GetStr(`h`)
  if line!= ``{
    LapLog("[HTTP] headers line: %s",line)
    for _,field:=range(strings.Split(line,`,`)){
      if(len(field)<3){continue}
      //LapLog("...... head: %s (%s)",field,field[1:2])
      if field[1:2]==`:` {
        name:=strings.TrimSpace(field[2:])
        LapLog("[HTTP] CSV header name: %s",name)
        a.OutFields=append(a.OutFields,name)
        if field[0:1]==`n` {
          a.field_types[name]=LapNum
        }else{
          a.field_types[name]=LapStr
        }
      }
    }
  }else{
    LapLog("No headers (h field)!")
    panic("ooops")
  }
  return &a
}

func (p *LapHttpReadCSV) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
