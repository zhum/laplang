package lap_processor

import (
//  "log"
//  "os"
//  "io"
  //"net/http"
  "net"
//  "bufio"
  "bytes"
  "encoding/binary"
  "strconv"
  "fmt"
)
//////////////////////////////////////////////////////////////////////
//    
//    LapDataProcessor for reading http stream
//    
//////////////////////////////////////////////////////////////////////
type LapHttpReadHopsa struct {
  LapDataCommon

  res       *net.UDPConn
  addr      *net.UDPAddr
  field_types map[string]int8
  id        string
  cnt       uint64
}

func (p LapHttpReadHopsa) GetSize() string {
  return fmt.Sprintf(`%ld`,p.cnt)
}


func (p LapHttpReadHopsa) fillInfo(info *map[string]string) {
  (*info)["type"]="read_hopsa"
}

// type UValue union {
//     uint64 b8[1]
//     uint32 b4[2]
//     uint16 b2[4]
//     uint8  b1[8]

//     float32 f4[2]
//     float64 f8[1]
// }

type E_VAL_TYPE uint8
const (
    t_uint8=0+iota
    t_uint16
    t_uint32
    t_uint64
    t_int8
    t_int16
    t_int32
    t_int64
    t_float32
    t_float64
    )

type SPacket struct {
    Value [8]byte //UValue

    Speed float64 // ??? TODO: make separate sensor?

    Address [4]uint8 // IPv4

    Server_timestamp uint32
    Server_usec uint32

    Agent_timestamp uint32
    Agent_usec uint32

    Sensor_id uint16
    Sensor_num uint8

    Version uint8

    Kind E_VAL_TYPE
}

func (p LapHttpReadHopsa) StartWork(*LapNode, *LapData, string) {
}

func (p *LapHttpReadHopsa) Input(n *LapNode, d *LapData, src string) {
  var data LapData
  //var err error
  var num float64
  var num_f32 float32
  var num_i16 uint16
  var num_i32 uint32
  var num_i64 uint64
  var pack SPacket
  //var counter int
  //counter=0

  if d.Cmd == `x` {
    p.Parent.SendToAll(d)
    return
  }

  //defer p.body.Close()
  if p.res==nil {
    LapLog("No hopsa srteam...")
    //p.Parent.SendToAll(d)
    //return
    conn, err := net.ListenUDP(`udp`,p.addr)
    if err!=nil {
      LapLog("Cannot listen address %s (code=%v)", conn, err)
      //p.body=nil
    }else{
      p.res=conn
      LapLog("HOPSA connected: %v",p.res)
    }

  }
  //LapLog("HOPSA READ = %+v", p.res)
  //r := bufio.NewReader(p.res)
  buf:=make([]byte,1024)
  for {
    // if(counter>128){
    //   counter=0
    //   LapLog("GOT 128!")
    // }
    length,_,_:=p.res.ReadFromUDP(buf)
    if length<=0 {
      continue
    }
    r:=bytes.NewReader(buf)
    p.cnt+=1
    if binary.Read(r, binary.LittleEndian, &pack)!=nil {
      continue
    }
//    LapLog("HOPSA RD=%+v",pack)
    if pack.Sensor_id==0{
      continue
    }
//    p.cnt+=1

    data = NewLapData(`[`)//, p.Parent.Name, ``)
    value_buffer:=bytes.NewReader(pack.Value[:])
    switch pack.Kind{
      case t_uint8:
      case t_int8:
        num=float64(pack.Value[0])
      case t_uint16:
      case t_int16:
        binary.Read(value_buffer,binary.LittleEndian, &num_i16)
        num=float64(num_i16)
      case t_uint32:
      case t_int32:
        binary.Read(value_buffer,binary.LittleEndian, &num_i32)
        num=float64(num_i32)
      case t_uint64:
      case t_int64:
        binary.Read(value_buffer,binary.LittleEndian, &num_i64)
        num=float64(num_i64)
      case t_float32:
        binary.Read(value_buffer,binary.LittleEndian, &num_f32)
        num=float64(num_f32)
      case t_float64:
        binary.Read(value_buffer,binary.LittleEndian, &num)
    }
    data.AddNumber(`value`,num)
    data.AddString(`address`,fmt.Sprintf("%d.%d.%d.%d",
      pack.Address[0],
      pack.Address[1],
      pack.Address[2],
      pack.Address[3]))

    data.AddNumber(`time`,float64(pack.Server_timestamp)*1000000+float64(pack.Server_usec))
    data.AddNumber(`time_a`,float64(pack.Agent_timestamp)*1000000+float64(pack.Agent_usec))
    data.AddNumber(`id`,float64(pack.Sensor_id))
    data.AddNumber(`n`,float64(pack.Sensor_num))
    p.Parent.SendToAll(&data)
  }
}

func (p *LapHttpReadHopsa) NewProcessor(d LapData) LapDataProcessor{
  a:=LapHttpReadHopsa{}
  a.field_types=make(map[string]int8)

  //LapLog("NEW FILE: %s",StrAny(``,` `,d))
  //a.fill_out_fields(d)

  // dd,ok:=d.GetStr(`delim`)
  // if ok {
  //   p.delim=dd
  // }else{
  //   p.delim=`;`
  // }

  proto,_:=d.GetStr(`proto`)
  LapLog("HOPSA proto=%s",proto)
  if proto==`` {
    proto=`udp`
  }

  fname,_:=d.GetStr(`addr`)
  if fname==`` {fname=`0.0.0.0`}
  port_str,_:=d.GetStr(`port`)
  if port_str==`` {port_str=`4499`}
  port,_:=strconv.ParseUint(port_str,64,0)
  LapLog("HOPSA address=%s:%s (%s)",fname,port,port_str)

  a.addr,_ = net.ResolveUDPAddr("udp",fmt.Sprintf("%s:%s",fname,port_str))
    //     Port: int(port),
    //     IP: net.ParseIP(fname),
    // }
  if fname!= `` {
    //a.fd,_=os.Open(fname)
    conn, err := net.ListenUDP(proto,a.addr)
    if err!=nil {
      LapLog("Cannot open address %s (code=%v)", fname, err)
      a.res=nil
      //p.body=nil
    }else{
      a.res=conn
      a.res.SetReadBuffer(1048576)
      LapLog("HOPSA listen: %+v",a.res)
    }
  }
  // line,_:=d.GetStr(`h`)
  // if line!= ``{
  //   LapLog("[HOPSA] headers line: %s",line)
  //   for _,field:=range(strings.Split(line,`,`)){
  //     if(len(field)<3){continue}
  //     //LapLog("...... head: %s (%s)",field,field[1:2])
  //     if field[1:2]==`:` {
  //       name:=strings.TrimSpace(field[2:])
  //       LapLog("[HOPSA] CSV header name: %s",name)
  //       a.OutFields=append(a.OutFields,name)
  //       if field[0:1]==`n` {
  //         a.field_types[name]=LapNum
  //       }else{
  //         a.field_types[name]=LapStr
  //       }
  //     }
  //   }
  // }else{
  //   LapLog("No headers (h field)!")
  //   panic("ooops")
  // }
  a.cnt=0
  return &a
}

func (p *LapHttpReadHopsa) SetId(s string){
  p.id=s
  LapLog("Updated id: %v",p)
}
