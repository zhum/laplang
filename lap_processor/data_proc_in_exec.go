package lap_processor

import (
//  "log"
//  "os"
  "io"
//  "net/http"
  "os/exec"
  "bufio"
  "strings"
  "strconv"
)
//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor for reading from executed program
//    
//////////////////////////////////////////////////////////////////////
type LapExecRead struct {
  LapDataCommon

  res       *io.ReadCloser
  skip      int64
  skipped   bool
  field_types map[string]int8
  delim     string
  id        string
}

func (p LapExecRead) GetSize() string {
  return "const"
}

func (p LapExecRead) fillInfo(info *map[string]string) {
  (*info)["type"]="exec_read"
  (*info)["skip"]=strconv.FormatInt(p.skip,10)
  (*info)["delim"]=p.delim
}


func (p *LapExecRead) execRead() {
  var data LapData
  var err error
  var num float64
  var bytes []byte

  //defer p.body.Close()
  if p.res==nil {
    LapLog("No executed program")
    panic("ooops")
  }
  //LapLog("EXEC READ = %+v", p.res)
  r := bufio.NewReader(*p.res)
  if ! p.skipped {
    for i:=int64(0);i<p.skip;i+=1 {
      _,_,err=r.ReadLine()
      if(err!=nil){
        LapLog("ERROR: %s",err)
      }
    }
  }
  p.skipped=true
  LapLog("EXEC SKIPPED OK")
  
  err=nil
  for err == nil {
    bytes, _, err = r.ReadLine()
    line:=string(bytes[:])
    fields:=strings.Split(line,p.delim)
    if len(fields)<len(p.OutFields) {
      LapLog("EXEC ERROR: bad string (%s) %d != %d (%v); skip",line,len(fields),len(p.OutFields),p.OutFields)
      continue
    }
    //LapLog("EXEC READ out=%v fields: %v",p.OutFields,fields)
    data = NewLapData(`[`)//, p.Parent.Name, ``)
    for i,f:=range(p.OutFields){
      if p.field_types[f]==LapNum {
        num,err=strconv.ParseFloat(fields[i],64)
        data.AddNumber(f,num)
      }else{
        data.AddString(f,strings.TrimSpace(fields[i])) //FIXME: remove trimming?
      }
    }
    //LapLog("**** '%+v'",p)
    //LapLog("[%s] EXEC Sending '%+v'",p.Parent.Name,data)
    p.Parent.SendToAll(&data)

//    bytes, _, err = r.ReadLine()
//    line=string(bytes[:])
  }
  LapLog("[EXEC] Error! %s",err)
//  p.res.Close()
  if err != io.EOF {
    LapLog("Error while http reading: %s",err)
    return
  }
  LapLog("[EXEC] EOF")
  data=NewLapData(`x`)
  p.Parent.SendToAll(&data)
}

func (p *LapExecRead) Input(n *LapNode, d *LapData, src string) {
  p.Parent=n
  LapLog("EXEC INPUT! %s",d.Cmd)
  if d.Cmd == `x` {
    p.Parent.SendToAll(d)
    return
  }
  go p.execRead()

}

func (p *LapExecRead) StartWork(n *LapNode, d *LapData, src string) {

  LapLog("EXEC START WORK! %s",d.Cmd)
  p.Parent=n
  go p.execRead()

}

func (p *LapExecRead) NewProcessor(d LapData) LapDataProcessor{
  a:=LapExecRead{}
  a.field_types=make(map[string]int8)

  //LapLog("NEW FILE: %s",StrAny(``,` `,d))
  //a.fill_out_fields(d)

  dd,ok:=d.GetStr(`delim`)
  if ok {
    a.delim=dd
  }else{
    a.delim=`;`
  }

  fname,_:=d.GetStr(`cmd`)
  cmdfull:=strings.Split(fname," ")
  LapLog("NEW EXEC=%+v",cmdfull)
  if fname!= `` {
    //a.fd,_=os.Open(fname)
    cmd:=exec.Command(cmdfull[0], cmdfull[1:]...)
    stdout, err := cmd.StdoutPipe()
    if err!=nil {
      LapLog("Cannot run %s (code=%d)", fname, err)
    }else{
      a.res=&stdout
      cmd.Start()
      //LapLog("Started (%v)",a.res)
    }
  }
  line,_:=d.GetStr(`h`)
  if line!= ``{
    LapLog("[EXEC] headers line: %s",line)
    for _,field:=range(strings.Split(line,`,`)){
      if(len(field)<3){continue}
      //LapLog("...... head: %s (%s)",field,field[1:2])
      if field[1:2]==`:` {
        name:=strings.TrimSpace(field[2:])
        LapLog("[EXEC] CSV header name: %s",name)
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
  skip:=d.ToNum(`skip`)
  a.skip=int64(skip)
  a.skipped=false
  return &a
}

func (p *LapExecRead) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
