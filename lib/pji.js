'use strict';

const commandLineArgs = require('command-line-args');
const commandLineUsage = require('command-line-usage');
const pg = require('pg');
const fs = require('fs');
const _ = require('underscore-node');

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
    client.query(query, (err,res) => {
      if (err) {
      //  if(err.message.indexOf('violates foreign key constraint')> -1){
          console.log('error message' , err.message , 'query error' ,query );         
         return reject(err);
      }
      resolve(res);
    });
  });
};

const displayUsage = function() {
  console.info(usage);
}

const exit = function(code) {
  process.exit(code);
}


const options = commandLineArgs(args);
let fieldsArr = [];


if (Object.keys(options).length === 0 || options.help) {
  displayUsage();
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
    // .pipe(JSONStream.parse('*'));
       .pipe(JSONStream.parse(JSONStream.stringifyObject()));

      stream.on('data', (data) => {
        return _.map(data, function(val,key){  
          if(options.relatedtable){
            let parentkey = _.extend(val, {[options.key]: key});
              _.each(val[options.relatedtable],(innertbl,key) =>{ 
                  // add key to inner table object
                   let clonestudiouser = _.clone(_.extend(parentkey,_.extend(innertbl, {[options.foreigkeys]: key})));

                   rows.push(clonestudiouser);
                })
            }else       
           rows.push(_.extend(val, {[options.key]: key}))
        });
        
      });
      stream.on('end', () => {
        console.info("Finished loading JSON...");
        //rowCount = rows.length;
        resolve(rows);
      });
      stream.on('error', (err) => reject(err));
    });
  })
  .then((rows) => {
    rowCount = rows.length;
    if(options.relatedtable){    
      // check if foreign key exists 
    // select all nested table from pg in array 
      const selectqueryTemplate = 'SELECT * FROM %I ;';
    return executeQuery(client, format(selectqueryTemplate, options.relatedtable)).then((rslt) => {
        var relatedkeys =_.pluck(rslt.rows, [options.foreigkeys]); 
         var filteredrows = _.reject(rows, function(row){ 
             // delete users node 
            delete row[options.relatedtable];        
            //return ((_.has(row, options.foreigkeys) && !_.contains(relatedkeys, row[options.foreigkeys])) || (_.has(row, 'invited_by')  && !_.contains(relatedkeys, row['invited_by'])) || (_.has(row, 'approved_by')  && !_.contains(relatedkeys, row['approved_by']))) ? true : false;
            if(_.has(row, 'invited_by')  && !_.contains(relatedkeys, row['invited_by'])){
              row['invited_by'] = row.settings.studio_owner ? row.settings.studio_owner : null ;
            }
            if(_.has(row, 'approved_by')  && !_.contains(relatedkeys, row['approved_by'])){
              row['approved_by'] = row.settings.studio_owner ? row.settings.studio_owner : null;
            }
            return (_.has(row, options.foreigkeys) && !_.contains(relatedkeys, row[options.foreigkeys]))  ? true : false;                 
          });
        rowCount = filteredrows.length;
        return filteredrows;
      });
    }
    return rows;
  })
  .then((rows) => {
    console.info('Inserting data...');
    return Promise.all(rows.map((row) => {
     let cols = fieldsArr.length>0 ? fieldsArr:Object.keys(row);
      // get values of columns in json file 
      let values =  cols.map((col) => {
        let  valu = jsonPath({json: row, path: "$.." + col , wrap :true});
        // if valuearray length in path >0 , check if empty , return null (foriegkey constraint not allow empty values)
        return  valu.length >0 ? valu[0] ? valu[0].toString().replace(',', '\,') :null :null;
      });


   // check if there is mapped fields
      
   let dbcols = [];
      if(options.databasefields){     
          cols.map(function(dbcol, i) {
            dbcols.push(options.databasefields.split(',')[i]);
        });
       
     }else
     dbcols = cols;
        return executeQuery(client, format(queryTemplate, options.table, dbcols, values));
    }));
  })
  .then(() => executeQuery(client, "COMMIT;"))
  .then(() => {
    console.info(`${rowCount} rows imported into the '${options.table}' table.`);
    exit(0);
  })
  .catch((error) => {
      return executeQuery(client, "ROLLBACK;").then((client) => {
         console.error(`There was an error importing into ${options.table}. The transaction was reversed. Details: \n\n ${error.toString()}`);
         exit(1);
       })
    
  });
});

