package lap_processor

import (
//  "log"
//  "os"
//  "io"
  "net/http"
  "net/url"
  "text/template"
  "strconv"
//  "bytes"
//  "encoding/binary"
//  "encoding/json"
  "strings"
  "fmt"
)

/********************************************************************

  /            -> = /static/index.html
  /static/     -> files from 'static' directory
  /cmd/COMMAND -> commands:
      show   -> show current graph
      new    -> create new node
      del    -> delete node

*********************************************************************/

//////////////////////////////////////////////////////////////////////
//    
//    Get commands via http and sent them to others
//    
//////////////////////////////////////////////////////////////////////
var head_node *LapNode

var data_pass map[string] string
var req_counter int

func str_head(s string,n int) string {
  max:=len(s)
  if(max>n){max=n}
  return s[0:max]
}

func str_tail(s string,n int) string {
  max:=len(s)
  if(max>n){max=n}
  return s[max:]
}

/////////////////////////////////////////////////////////
//
// Handle all requests
//
func LapHttpAll(w http.ResponseWriter, req *http.Request) {
  path:=req.URL.Path

  LapLog("[HTTP]****** Got '%s'",path)
  path=str_tail(path,1)
  if path==`` {
    LapHttpindex(w,req)
    return
  }
  if str_head(path,3) == `cmd` {
    LapHttpCmdServer(w,req)
    return
  }
  if str_head(path,6) == `static` {
    LapHttpStaticFiles(w,req)
    return
  }
  req.URL.Path="/static/"+req.URL.Path
  LapHttpStaticFiles(w,req)
  return
  //http.Error(w, fmt.Sprintf("[HTTP] Oops... path=%s",path), http.StatusNotFound)
}

///////////////////////////////////////////////////////////////
//
// Handle /static/*
//
func LapHttpStaticFiles(w http.ResponseWriter, r *http.Request) {
  //w.Header().Set("Content-Type", "text/html; charset=utf-8")
  path:=r.URL.Path[8:]
  path=strings.Replace(path,"/..","/#",-1)
  LapLog("[HTTP] File '%s' (%s)",r.URL.Path,path)

  http.ServeFile(w, r, "static/"+path)
}

///////////////////////////////////////////////////////////////
//
// Handle /index.html
//
func LapHttpindex(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "text/html; charset=utf-8")
  LapLog("[HTTP] Index")
  http.ServeFile(w, r, "static/index.html")
}

type show_data struct{
  Js    string //template.JS
  Graph string //template.HTML
}

/////////////////////////////////////////////////////////
//
// Process 'show' command
//
func http_show(w http.ResponseWriter, req *http.Request,
               parent string, target string, str_args map[string]string) (string,bool){
  
  LapLog("================>  SHOW COMMAND")
  data:=NewLapData("v", `to`, `head`)
  str_counter:=fmt.Sprintf("%d",req_counter)
  data.AddString(`index`,str_counter)
  head_node.Input(&data,``)
  tmpl,ok:=template.ParseFiles("show.tmpl")
  if ok!=nil {
    LapLog("TEMPLATE ERROR: %s",ok.Error())
  }
  d:=show_data {
    data_pass[str_counter+"js"], //template.JS(data_pass[str_counter+"js"]),
    data_pass[str_counter], //template.HTML(data_pass[str_counter]),
  }

  LapLog("FILTERS LIST: %s",data_pass[str_counter+"js"])
  LapLog("NODES LIST:\n%s",data_pass[str_counter])

  tmpl.Execute(w,d)
  delete(data_pass,str_counter)
  delete(data_pass,str_counter+"js")
  return ``,false
}

/////////////////////////////////////////////////////////
//
// Process 'new' command
//
func http_new(w http.ResponseWriter, req *http.Request,
              parent string, target string, str_args map[string]string) (string,bool){
  
  LapLog("================>  NEW COMMAND")
  data:=NewLapData(`n`, `to`, target)
  for k,v:= range str_args {
    data.AddString(k,v)
  }
  LapLog("Sending %+v",data)
  head_node.Input(&data,``)
  return ``,false
}

/////////////////////////////////////////////////////////
//
// Process 'connect' command
//
func http_connect(w http.ResponseWriter, req *http.Request,
                  parent string, target string, str_args map[string]string) (string,bool){
  
  LapLog("================>  CONNECT COMMAND")
  data:=NewLapData("c", `to`, target)
  for k,v:= range str_args {
    data.AddString(k,v)
  }
  LapLog("Sending %+v",data)
  head_node.Input(&data,``)
  return ``,false
}

/////////////////////////////////////////////////////////
//
// Process 'del' command
//
func http_del(w http.ResponseWriter, req *http.Request,
              parent string, target string, str_args map[string]string) (string,bool){
  
  LapLog("================>  DEL COMMAND")
  targets:=make([]string,0)
  if _,ok:=str_args[`list`]; ok {

    targets=strings.Split(str_args[`list`],`,`)
  }

  data:=NewLapData(`x`, `to`, target)
  head_node.Input(&data,``)

  for i:=len(targets)-1; i>=0; i-- {
    data:=NewLapData("d", `to`, targets[i])
    for k,v:= range str_args {
      data.AddString(k,v)
    }
    head_node.Input(&data,``)
  }
  return `ok`, false
}

/////////////////////////////////////////////////////////
//
// Process 'filter' command
//
func http_filter(w http.ResponseWriter, req *http.Request,
                 parent string, target string, str_args map[string]string) (string,bool){
  
  LapLog("================>  FILTER COMMAND")
  filter:=str_args[`filter_strings`]
  if filter==`` {
    from,err:=strconv.ParseFloat(str_args[`filter_from`],64)
    if err!=nil {
      LapLog("No filter args specified... Skip (%+v)",err)
      http.Redirect(w, req, "/index.html", http.StatusFound)
      return ``,false
    }else{
      to:=float64(0)
      to,_=strconv.ParseFloat(str_args[`filter_to`],64)
      filter=fmt.Sprintf("[%f %f]",from,to)
    }
  }
  name:=str_args[`name`]
  field:=str_args[`field`]

  LapLog("Setting filter '%s' to node %s",filter,name)
  data:=NewLapData("f", `to`, target)
  data.AddString(`filter`,filter)
  data.AddString(`field`,field)
  data.AddString(`name`,name)
  head_node.Input(&data,``)
  rq:="/cmd?cmd=show&msg="+url.QueryEscape(`ok`)
  http.Redirect(w, req, rq, http.StatusFound)
  return ``,false
}

////////////////////////////////////////////////////////
//
//  Handle /cmd&cmd=...
//
func LapHttpCmdServer(w http.ResponseWriter, req *http.Request) {
  w.Header().Set("Content-Type", "text/html; charset=utf-8")

  if data_pass==nil {
    data_pass=make(map[string]string)
  }

  var parent string
  var target string
  var str_args map[string]string // args, passed via query (name1=abc&arg2=qwe&...)

  parent="head"
  target=""

  str_args=make(map[string]string)
  // parse query...
  v:=req.URL.Query()
  for key, value := range v {
    switch key {
    case "parent":
      parent=value[0]
      break
    case "target":
      target=value[0]
      break
    default:
      LapLog("ARG: %s -> %s",key,value[0])
      str_args[key]=value[0]
    }
  }

  LapLog("[HTTP] CMD '%s'",req.URL.Path)
  path:=str_tail(req.URL.Opaque,1)
  if path=="" || path=="/" {
    path=str_args["cmd"]
  }
  LapLog("[HTTP] Got '%s' (%s)",path, req.URL.Path)

  switch path {
  case "show":
    http_show(w, req, parent, target, str_args)
    break
  case "new":
    http_new(w, req, parent, target, str_args)
    break
  case "del":
    http_del(w, req, parent, target, str_args)
    break
  case "filter":
    http_filter(w, req, parent, target, str_args)
    break
  case "connect":
    http_connect(w, req, parent, target, str_args)
    break
  default:
    LapLog("================> BAD COMMAND")
    http.Error(w,"No such command",http.StatusNotFound)
  }
}

func Lap_http_start(n *LapNode, port int) {
  head_node=n
  //http.HandleFunc("/static", LapHttpStaticFiles)
  //http.HandleFunc("/cmd", LapHttpCmdServer)
  http.HandleFunc("/", LapHttpAll)
  port_string:=fmt.Sprintf(":%d",port)

  go func(){
    err := http.ListenAndServe(port_string, nil)
    if err != nil {
      LapLog("ListenAndServe failed: ", err)
    }
  }()
  // go func(){
  //   err := http.ListenAndServeTLS(port_string, "cert.pem", "key.pem", nil)
  //   if err != nil {
  //     LapLog("ListenAndServeTLS failed: ", err)
  //   }
  // }()
}