'use strict';

const commandLineArgs = require('command-line-args');
const commandLineUsage = require('command-line-usage');
const pg = require('pg');
const fs = require('fs');
const _ = require('underscore-node');
const objectPath = require("object-path");
var jsonPath  = require('jsonpath-plus');
const format = require('pg-format');
const JSONStream = require('JSONStream');
const args = require('./args');

const usage = commandLineUsage([
  {
    header: 'pg-json-import',
    content: 'Import data from a JSON file into a PG table'
  },
  {
    header: 'Synopsis',
    content: [
      '$ pji [bold]{--file} [underline]{filename.json} [bold]{-c} [underline]{postgresql://localhost:5432/mydb} [bold]{-t} [underline]{tablename}',
      '$ pji [bold]{--file} [underline]{filename.json} [bold]{--database} [underline]{dbname} [bold]{--table} [underline]{tablename}',
      '$ pji [bold]{--file} [underline]{filename.json} [bold]{--host} [underline]{host} [bold]{--port} [underline]{port} [bold]{--database} [underline]{dbname} [bold]{--table} [underline]{tablename}'
    ]
  }
]);

const queryTemplate = 'INSERT INTO %I (%I) VALUES (%L);';

const executeQuery = function(client, query) {
  return new Promise((resolve, reject) => {
    client.query(query, (err) => {
      if (err) {
        console.log('error',query);
        return reject(err);
      }
      resolve();
    });
  });
};
//console.log('node pji --file irehearsemarwa.json -c postgresql://postgres:123456@localhost:5432/iRehearse  -t copyusers');

const displayUsage = function() {
  console.info(usage);
}

const exit = function(code) {
  process.exit(code);
}


const options = commandLineArgs(args);
//options.file = 'F:/Free/data migration/pg-json-import-master/lib/irehearsemarwa.json';
///options.table = 'copyusers';
//options.connection = 'postgresql://postgres:123456@localhost:5432/iRehearse';
let fieldsArr = [];


if (Object.keys(options).length === 0 || options.help) {
  displayUsage();
  //exit('pji --file irehearsemarwa.json -c postgresql://postgres:123456@localhost:5432/iRehearse  -t copyusers');
  exit(0);

}

if (!options.file || !options.file.exists) {
  console.error('You must provide a valid JSON file to open with -f or --file');
  exit(1);
}

if (!options.connection) {
  console.error('You must provide a connection string for PostgreSQL with -c or --connection');
  exit(1);
}

if (!options.key) {
  console.error('You must provide primary field name  with -k or --key');
  exit(1);
}

if (!options.table) {
  console.error('You must specify a destination table for the data using -t or --table');
  exit(1);
}

if (!options.node) {
  console.error('You must specify a source json node for the data using -n or --node');
  exit(1);
}
if (options.fields) {
 /// console.error('You must specify a destination table for the data using -t or --table');
 // exit(1);
 fieldsArr =  options.fields.split(',');
}

pg.connect(options.connection, (err, client, done) => {
  if (err) {
    console.error(`There was a problem connecting`, err);
    exit(1);
  }

  let rowCount;


  executeQuery(client, "BEGIN;")
  .then(() => {
    return new Promise((resolve, reject) => {
      console.info("Loading JSON...");
      let rows = [];
      let stream = fs.createReadStream(options.file.filename)
        .pipe(JSONStream.parse(options.node));
        //.pipe(JSONStream.parse('*'));

      stream.on('data', (data) => {
        return _.map(data, function(val,key){  
          if(options.relatedtable){
            let parentkey = _.extend(val, {[options.key]: key});
          let relatedtable = val[options.relatedtable]
             return _.map(relatedtable, function(fval,fkey){
                rows.push(_.extend(parentkey,_.extend(fval, {[options.foreigkeys]: key})));
                delete parentkey[options.relatedtable];
              })

              var tt = rows;
            // let  foreigkeys= options.foreigkeys.split(',');
            //    foreigkeys.forEach(fkey => {
            //     var nestedtable = fkey.split(":")[1];
            //     var foreigkey = fkey.split(":")[0];
            //     _.extend(val, {[options.key]: key})

            //    })
            }else         
          rows.push(_.extend(val, {[options.key]: key}))
        });
        // 
     // rows.push(data)
      });
      stream.on('end', () => {
        console.info("Finished loading JSON...");
        rowCount = rows.length;
 
        resolve(rows);
      });
      stream.on('error', (err) => reject(err));
    });
  })
  .then((rows) => {
    console.info('Inserting data...');
   //var ioi =  _.extend(jsonPath({json: rows, path: "$..users" }), rows);
    return Promise.all(rows.map((row) => {
     let cols = fieldsArr.length>0 ? fieldsArr:Object.keys(row);

      // if(fieldsArr.length > 0){
      //    cols = cols.map((field) =>  !field.indexOf(':') == -1 ? field.split(':')[1] : field.split(':')[0] );
      //  }  
      //let values = cols.map((col) => {

      let newcols = []; 

      let values =  cols.map((col) => {
       //check nesting in foreig table
       if(col.indexOf(':') > -1) 
       col = col.split(':')[1];
        //jsonPath({json: row, path: "$.." + col , wrap :true}).length ?   jsonPath({json: row, path: "$.." + col})[0].toString().replace(',', '\,'):null}
         var va =  jsonPath({json: row, path: "$.." + col , wrap :true}).length >0 ?  jsonPath({json: row, path: "$.." + col,wrap :true})[0].toString().replace(',', '\,') :null;
         return va;
      });


      _.each(cols,(col,index) =>{ 
            if(col.indexOf(':') > -1) 
             cols[index] = col.split(':')[0];
        });
      
      console.log(cols , 'cols')
      console.log(values , 'values')
  return executeQuery(client, format(queryTemplate, options.table, cols, values));
    }));
  })
  .then(() => executeQuery(client, "COMMIT;"))
  .then(() => {
    console.info(`${rowCount} rows imported into the '${options.table}' table.`);
    exit(0);
  })
  .catch((error) => {
    executeQuery(client, "ROLLBACK;").then(() => {
      console.error(`There was an error importing into ${options.table}. The transaction was reversed. Details: \n\n ${error.toString()}`);
      exit(1);
    })
  });
});

//console.log('pji --file irehearsemarwa.json -c postgresql://postgres:123456@localhost:5432/iRehearse  -t copyusers');
