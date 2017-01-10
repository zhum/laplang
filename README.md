Internal Commands
=================

All data and commands are passed via LapData struct. Its fields are:

  Cmd    string     // command ('[' for data)
  Id     int64      // almost uniq message ID
  From   string     // sender
  To     string     // receiver
  Fields map[string]*LapField // data

Commands
--------

- [ Data
- n New child
- f Set filter
- c Connect output to node
- d Delete node
- v View all nodes
- e Send EOD to node

New child
---------

- name = new node name
- type = new node processor

Set filter
----------

- name = target node
- field = name of field to filter
- filter = filter description

Filter can be specified as list of strings/integers via comma or as integer interval: '[98 123]'

TODO: make intervals float, add multiple intervals, add open intervals

Connect output
--------------

- target = node, to which connect out output

Delete node
-----------

- list = list of target node names via COMMA

View all nodes
--------------

no options

Send EOD
--------

no options

Data processors (names)
---------------
By default all processors copy all input fields values. In aggregation processors only last values are copied. In special cases only specified out fields go to output.

- avg = compute average value till EOD
    - count_field = field to compute avg
    - out_field = new name for aggregated field 
- min = compute minimum value till EOD
    - like avg
- max  = compute maximum value till EOD
    - like avg
- print = output data AS IS. Just for debugging
  - none
- file = read csv-file
    - filename = name of input file
    - headers = name of file with headers description (T:name,..., where T is
      'n' for number and 's' for string)
- http_csv = read csv from http (not tested)
    - delim = fields delimiter
    - addr = address
    - h = headers description (as is) - see 'file' processor
- http_hopsa = not works
- exec = read data from executed program
    - delim = fields delimiter
    - cmd = command to execute
    - h = headers description (as is) - see 'file' processor
    - skip = number of lines to skip at first run
- outcsv = print data to csv-file
    - filename = name of output file
    - out_fields = list of fields to put in file
    - print_header = 1/0 (y/n) to print header line or not
    - print_source = add column with data source name
- null = put data to /dev/null
- slice = put EOD every N data pieces or/and by delta in one field
    - count = number of data pieces
    - delta_field = name of field to watch delta
    - delta = value of delta
- agr = do aggregation on one field
    - count_field = name of field by which do aggregation
    - out_field = name of aggregation field in output
    - grp_field = name of field to be uniq in aggregation
    - agr_type = type of aggregation: min, max or avg
    - maxbuf = maximum number of data pieces while aggregation
- sort = local sort data by one field
    - sort_field = name of field to sort by
    - sort_descend = if specified (any value, FIXME), sort descending
    - max_count = maximum data pieces in sort pool
- grid = do aggregation by several fields inside intervals, specified in one field
    - by_field = name of field, which specify aggregation interval
    - max_count = maximum number of intervals to store in memory
    - max_hole = maximum number of intervals, which will be tried to fill
    - start = start value for aggregation field
    - step = width of aggregation interval
    - save_fields = fields to copy in output via comma
    - grid_fields = fields to be uniq inside interval via comma
- join = just join several sources data into one piece
    - by_fields = list of fields, to be uniq in joining
    - sources = list of source nodes via comma
    - maps = list of source fields and their new names in format source_node/source_field/new_field,...
