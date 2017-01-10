package lap_processor

import (
//  "log"
  "strings"
//  "os"
)
/*
    Data Processors

    Each data processor must implement LapDataProcessor interface
    and have global function NewXXXXXX, which returns LapDataProcessor.
    This function must receive LapData as argument ('n' command), which can
    carrier parameters for new procesor (processing field name, etc.)

*/

type LapDataProcessor interface {
  Input(*LapNode, *LapData, string)
  StartWork(*LapNode, *LapData, string)
  FinishWork(*LapNode, *LapData, string)
  SetId(string)
  SetParent(*LapNode)
  GetSize() string
  fillInfo(*map[string]string)

  NewProcessor(LapData) LapDataProcessor
}

//////////////////////////////////////////////////////////////////////
//
//  Common part for DataProcessors
//  Contains Parent node reference and ouptput fields names array
//
//////////////////////////////////////////////////////////////////////
type LapDataCommon struct {
  Parent     *LapNode
  OutFields  []string
}

func (p *LapDataCommon) copy_out_fields(from *LapData, to *LapData) {
  // defer func(){
  //   err:=recover()
  //   if err==nil {
  //     err=`unknown error`
  //   }
  //   LapLog("Failed at copy_out_fields: %s",err)
  // }()
  for i := range p.OutFields {
    s,ok:=from.StrFields[p.OutFields[i]]
    if ok{
      to.AddString(p.OutFields[i],s)
      //LapLog("== coped %s (%+v)",p.OutFields[i],from)
    }else{
      n,ok2:=from.NumFields[p.OutFields[i]]
      if ok2 {
        to.AddNumber(p.OutFields[i],n)
      }else{
        LapLog("Failed at copy_out_field %s: no such field",p.OutFields[i])
      }
    }
  }
}

func (p *LapDataCommon) fill_out_fields(d LapData) {
  if d.CheckField(`out_fields`) {
    f,_ := d.GetStr(`out_fields`)
    LapLog("Filled out fields for %+v",p)
    p.OutFields=strings.Split(f,`,`)
  } else{
    LapLog("Warning! No out fields!!! (%+v)",p)
    p.OutFields=[]string{}
  }
}

func (p *LapDataCommon) SetParent(pp *LapNode) {
  p.Parent=pp
}

func (p *LapDataCommon) SetId(string){
  LapLog("Fake setid")
}

func (p *LapDataCommon) GetSize() string {
  return "no size"
}

func (p *LapDataCommon) fillInfo(*map[string]string){LapLog("ERROR! generic fillInfo called!")}

func (p *LapDataCommon) FinishWork(*LapNode, *LapData, string) {
}

///////////////////////////////////////////////////////////////////
//
//  Example of Data processor: print all data
//
///////////////////////////////////////////////////////////////////
type LapPrint struct {
  LapDataCommon
}
func (p *LapPrint) SetId(s string) {
}
func (p *LapPrint) StartWork(*LapNode, *LapData, string) {
}
func (p *LapPrint) FinishWork(*LapNode, *LapData, string) {
}

func (p *LapPrint) Input(n *LapNode, d *LapData, src string) {
  if d.Cmd == `[` {
    LapLog("*** %s",d);//StrAny(``,` `,d.Fields))  
  }else{
    LapLog("*** COMMAND:%v",d.Cmd);//StrAny(``,` `,d.Fields))  
  }
}

func (p *LapPrint) GetSize() string {
  return "const"
}

func (p *LapPrint) fillInfo(info *map[string]string) {
  (*info)["type"]="print"
}


func (p *LapPrint)NewProcessor(d LapData) LapDataProcessor{
  return &LapPrint{}
}

