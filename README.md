# pg-json-import

pg-json-import lets you take data from a JSON file, and insert it into a table on a postgres DB

## Installation

Download zip folder and extract it .

Add JSON file in folder

open cmds.txt and Rename JSON file in each command (exisiting one is all.json) and change postgres connectionstring .

Run  commands at cmds.txt in sequence.

## Usage

```
  $ pji --file filename.json -c postgresql://localhost:5432/mydb -t tablename
  $ pji --file filename.json --database dbname --table tablename
  $ pji --file filename.json --host host --port port --database dbname --table tablename

  -f, --file file     The JSON file to load
  -c, --connection    A connection string to PostgreSQL. i.e. postgresql://localhost:5432/mydb
  -t, --table         The table name where data should be imported
  --help              show this message
```
