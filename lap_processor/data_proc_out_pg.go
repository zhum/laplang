package lap_processor

import (
  //  "log"
  //"os"
  //"io"
  //"io/ioutil"
  "fmt"
  "strings"
  //"strconv"
  "../helpers"
  //  "bytes"
  "database/sql"
  _ "github.com/lib/pq"
)

///////////////////////////////////////////////////////////////////
//
//  Data processor: put data to PGsql
//
///////////////////////////////////////////////////////////////////
type LapPGOut struct {
  LapDataCommon
  fields   []string
  n_values map[string]float64
  s_values map[string]string
  db       *sql.DB
  query    string
}

func (p LapPGOut) StartWork(*LapNode, *LapData, string) {
}

func (p *LapPGOut) SetId(s string) {
}

func (p LapPGOut) GetSize() string {
  return "const"
}

func (p LapPGOut) fillInfo(info *map[string]string) {
  (*info)["type"] = "pg_out"
  (*info)["fields"] = helpers.Reduce(p.fields, "",
    func(mem interface{}, val interface{}) interface{} {
      if mem.(string) == "" {
        return val.(string)
      }
      return mem.(string) + "," + val.(string)
    }).(string)
  (*info)["query"] = p.query
}

//var first_pg_put bool = true
func (p *LapPGOut) Input(n *LapNode, d *LapData, src string) {
  p.Parent = n
  vals := make([]interface{}, 0)

  if d.Cmd == `[` {
    //    LapLog("GOT DATA %+v",d)
    for _, fname := range p.fields {
      if n, ok := d.GetNum(fname); ok {
        vals = append(vals, n)
      } else {
        vals = append(vals, d.ToStr(fname))
      }

    }
    //if(first_pg_put){LapLog("PG: executing '%s'",p.query);first_pg_put=false}
    _, err := p.db.Exec(p.query, vals...)
    if err != nil {
      LapLog("PG error: %s", err)
    }

  } else if d.Cmd == `x` {
  } else {
    LapLog("[%s]*** COMMAND:%v", p.Parent.Name, d.Cmd) //StrAny(``,` `,d.Fields))
  }
}

func (p *LapPGOut) FinishWork(*LapNode, *LapData, string) {
  if p.db != nil {
    p.db.Close()
  }
}

func (p LapPGOut) NewProcessor(d LapData) LapDataProcessor {
  name, _ := d.GetStr(`dbconnect`)
  table, _ := d.GetStr(`table`)
  //"host=localhost port=5432 user=your_role password=your_password dbname=your_database sslmode=disable"
  ret := LapPGOut{}
  ret.fields = make([]string, 0)

  ret.query = fmt.Sprintf("insert into %s values (", table)

  db, err := sql.Open("postgres", name)
  ret.db = db
  //defer db.Close()

  if err != nil {
    LapLog("PG Database opening error -->%v\n", err)
    panic("Database error")
  }

  fields_string, ok := d.GetStr(`out_fields`)
  if ok {
    ret.fields = strings.Split(fields_string, ",")
  }

  delimiter := ``
  for i, _ := range ret.fields {
    ret.query = fmt.Sprintf("%s%s$%d", ret.query, delimiter, i+1)
    delimiter = `,`
  }
  ret.query = fmt.Sprintf("%s);", ret.query)

  return &ret
}
