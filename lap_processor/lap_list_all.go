//
// Processors for LapLang
//
package lap_processor

import (
  "fmt"
)

////////////////////////////////////////////////////////////////////////////
//
// Command 'list all nodes'
type LapListAllCommand struct {
}

func ListNodes() (string,string){
  var list string
  var tooltips string

  //LapLog("START List ALL")
  list="graph LR;\n"
  tooltips="{"
  delim:=``
  for src_name,src := range all_nodes {
    /*
    LapLog("LIST: NODE %s",src_name)
    for nam,nod:=range src.Node.parents {LapLog("  parent0: %s (%v)",nam,nod)}
    for _,nam2:=range src.parents {LapLog("  parent1: %s",nam2)}
    */

    tooltips=fmt.Sprintf(`%s%s"%s": {"info": %s,"links": %s, "filters": %s, "counters": %s}`,
                          tooltips,delim,src_name,
                          src.Node.jsonNodeInfo(),
                          src.Node.jsonNodeLinks(),
                          src.Node.jsonNodeFilters(),
                          src.Node.jsonNodeCounters())
    delim=", "
    for _,dst_name := range src.childs {
      //LapLog("LIST:  \\_ %s",dst_name)
      // dst_id:="xxx"
      // if _,ok:=all_nodes[dst_name]; ok {
      //   dst_id=fmt.Sprintf("%d",all_nodes[dst_name].Node.GetId())
      // }
      // //src_id:="___"
      // src_id:=fmt.Sprintf("%d",src.Node.GetId())

      src_id:=fmt.Sprintf("Node_%s",src_name)
      dst_id:=fmt.Sprintf("Node_%s",dst_name)
      src_id=src_name
      dst_id=dst_name

      arrow:="--"
      //t_list:="["
      //delim2:=""

      if filter,ok:=src.Node.childFilters[dst_name]; ok {

         for _,ff:=range filter{
           if ff.ToString()!=``{
             arrow="=="
           }
         }
          // if(ff!=nil){
          //   //LapLog("FF=%+v",ff)

          //   // FILTER text
          //   text=fmt.Sprintf("%s%s; ",text,ff.ToString())
          // }
        //   if text != "" {
        //     arrow="=="
        //     //tooltips+=fmt.Sprintf("$('#Node%s').tipsy({gravity: 'se', title: function(){return 'FILTER: %s';}});",src_id,text)
        //     t_list+=delim2+"'"+text+"'"
        //     delim2=", "
        //   }
        // }
        // if t_list != "[" {
        //   t_list="Node"+dst_id+": "+t_list+"]"
        // }
        // tooltips0+=delim0+t_list
        // delim0=", "
        //LapLog("Tooltips add '%s'",tooltips)
      // }
      // if tooltips0!=`{` {
      //   tooltips+=delim+"Node"+src_id+":"+tooltips0+"}"
      //   delim=", "
      }

      if dst_name=="" { dst_name="NONE" }
      list= fmt.Sprintf("%s  %s(%s) %s> %s(%s);\n",
        list,src_id,src_name,arrow,dst_id,dst_name)
    }
  }
  tooltips+=`}`

  return list,tooltips
}


// @proc - parent processor
// @d    - command with args
//
//
func (p *LapListAllCommand) Input(node *LapNode, d *LapData) (string, bool) {

  /********************************
  // Check by this:
  for i,node := range all_nodes {
    for j := range node.parents {
      list += fmt.Sprintf("  %s ---> (%s)\n",j,i)
    }
  }

  */
  list,tooltips:=ListNodes()

  index,_:=d.GetStr(`index`)
  data_pass[index]=list
  data_pass[index+"js"]=tooltips
  //LapLog("Tooltips='%s'",tooltips)

  return `ok`,false //true
}

func (p *LapListAllCommand) NewCommander() LapCommander{
  return new(LapListAllCommand)
}


// __END__
//   list="graph LR;\n"
//   tooltips="{"
//   delim:=``
//   for src_name,src := range all_nodes {
//     /*
//     LapLog("LIST: NODE %s",src_name)
//     for nam,nod:=range src.Node.parents {LapLog("  parent0: %s (%v)",nam,nod)}
//     for _,nam2:=range src.parents {LapLog("  parent1: %s",nam2)}
//     */
//     delim0:=``
//     tooltips0:=`{`
//     for _,dst_name := range src.childs {
//       //LapLog("LIST:  \\_ %s",dst_name)
//       dst_id:="xxx"
//       if _,ok:=all_nodes[dst_name]; ok {
//         dst_id=fmt.Sprintf("%d",all_nodes[dst_name].Node.GetId())
//       }
//       //src_id:="___"
//       src_id:=fmt.Sprintf("%d",src.Node.GetId())

//       arrow:="--"
//       t_list:="["
//       delim2:=""

//       if filter,ok:=src.Node.childFilters[dst_name]; ok {
//         for _,ff:=range filter{
//           if(ff!=nil){
//             //LapLog("FF=%+v",ff)

//             // FILTER text
//             text=fmt.Sprintf("%s%s; ",text,ff.ToString())
//           }
//           if text != "" {
//             arrow="=="
//             //tooltips+=fmt.Sprintf("$('#Node%s').tipsy({gravity: 'se', title: function(){return 'FILTER: %s';}});",src_id,text)
//             t_list+=delim2+"'"+text+"'"
//             delim2=", "
//           }
//         }
//         if t_list != "[" {
//           t_list="Node"+dst_id+": "+t_list+"]"
//         }
//         tooltips0+=delim0+t_list
//         delim0=", "
//         //LapLog("Tooltips add '%s'",tooltips)
//       }
//       if tooltips0!=`{` {
//         tooltips+=delim+"Node"+src_id+":"+tooltips0+"}"
//         delim=", "
//       }

//       if dst_name=="" { dst_name="NONE" }
//       list += fmt.Sprintf("  Node%s(%s) %s> Node%s(%s);\n",
//         src_id,src_name,arrow,dst_id,dst_name)
//     }

//   }
//   tooltips+=`}`
