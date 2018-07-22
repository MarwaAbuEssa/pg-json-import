'use strict';

const commandLineArgs = require('command-line-args');
const commandLineUsage = require('command-line-usage');
const pg = require('pg');
const fs = require('fs');
const _ = require('underscore-node');

var jsonPath = require('jsonpath-plus');
const format = require('pg-format');
const JSONStream = require('JSONStream');
const args = require('./args');
const moment = require('moment')

const usage = commandLineUsage([{
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

const executeQuery = function (client, query) {
  return new Promise((resolve, reject) => {
    client.query(query, (err, res) => {
      if (err) {
        //  if(err.message.indexOf('violates foreign key constraint')> -1){
        console.log('error message', err.message, 'query error', query);
        return reject(err);
      }
      resolve(res);
    });
  });
};

const displayUsage = function () {
  console.info(usage);
}

const exit = function (code) {
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
  fieldsArr = options.fields.split(',');
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
          return _.map(data, function (val, key) {
            // if (options.table == 'studentclasses') {
            //   let parentkey = _.extend(val, {
            //     [options.key]: key
            //   });

            // }
            if (options.relatedtable) {
              let parentkey = _.extend(val, {
                [options.key]: key
              });
              if (val[options.relatedtable]) {
                _.each(val[options.relatedtable], (innertbl, key) => {
                  let clonestudiouser;
                  if (options.relatedtable == 'comments') {
                    clonestudiouser = _.clone(_.extend(parentkey, innertbl));
                  } else if (options.relatedtable == 'applauses') {
                    clonestudiouser = _.clone(_.extend(parentkey, _.extend({
                      'user': key
                    }, {
                      'timestamp': innertbl
                    })));
                  } else if (options.relatedtable == 'students') {
                    clonestudiouser = _.clone(_.extend(parentkey, _.extend({
                      'studentKey': key
                    }, {
                      'studenttimestamp': innertbl
                    })));
                  } else {
                    clonestudiouser = _.clone(_.extend(parentkey, _.extend(innertbl, {
                      [options.foreigkeys]: key
                    })));
                  }
                  // add key to inner table object
                  rows.push(clonestudiouser);
                })

              }
            } else
              rows.push(_.extend(val, {
                [options.key]: key
              }))
          });

        });
        stream.on('end', () => {
          console.info("Finished loading JSON...");
          //rowCount = rows.length;
          console.log('loaded rows from json file ', rows.length);
          resolve(rows);
        });
        stream.on('error', (err) => reject(err));
      });
    })

    .then((rows) => {
      // check user key 
      rowCount = rows.length;
      //   if(options.table == 'photos' || options.table == 'videos'|| options.table == 'audios' || options.table == 'posts' || options.table == 'postcomments' ||  options.table == 'postapplauses'){
      if (_.contains(['photos', 'videos', 'audios', 'posts', 'postcomments', 'postapplauses', 'events', 'classes', 'studentclasses'], options.table)) {
        const selectqueryTemplate = 'SELECT * FROM %I ;';
        return executeQuery(client, format(selectqueryTemplate, 'users')).then((rslt) => {
          //  if (options.table == 'classes') {
          //  var relatedkeys = _.pluck(rslt.rows, 'teacherKey');
          //} else {
          var relatedkeys = _.pluck(rslt.rows, 'userKey');
          //}

          let filteredrows = [];
          // var filteredrows = _.reject(rows, function(row){ 
          // return (_.has(row, 'user') && !_.contains(relatedkeys, row['user']))  ? true : false;  
          // }); 
          _.each(rows, (tblrow) => {
            // if(tblrow['studioKey'] == "-K3rd4C81__oO4CpRm6q"){
            var row = _.clone(tblrow);
            if (options.table == 'classes') {
              // replace non existing value of userKey to null
              if ((_.has(row, 'teacher') && !_.contains(relatedkeys, row['teacher']))) {
                row['teacher'] = null;
              }
            } else if (options.table == 'studentclasses') {
              // replace non existing value of userKey to null
              if ((_.has(row, 'studentKey') && !_.contains(relatedkeys, row['studentKey']))) {
                row['studentKey'] = null;
              }
            } else {
              // replace non existing value of userKey to null
              if ((_.has(row, 'user') && !_.contains(relatedkeys, row['user']))) {
                row['user'] = null;
              }
            }

            filteredrows.push(row);
            //  }
          })
          rowCount = filteredrows.length;
          return filteredrows;
        })
      }
      return rows;

    })
    .then((rows) => {
      // check class key for related tables audio , video
      rowCount = rows.length;
      if (_.contains(['videos', 'audios', 'studentclasses'], options.table)) {
        const selectqueryTemplate = 'SELECT * FROM %I ;';
        return executeQuery(client, format(selectqueryTemplate, 'classes')).then((rslt) => {
          var relatedkeys = _.pluck(rslt.rows, 'classKey');
          let filteredrows = [];
          _.each(rows, (tblrow) => {
            var row = _.clone(tblrow);

            if ((_.has(row, 'class') && !_.contains(relatedkeys, row['class']))) {
              row['class'] = null;
            }

            filteredrows.push(row);
          })
          rowCount = filteredrows.length;
          return filteredrows;

        })
      }

      return rows;
    }).then((rows) => {
      // rowCount = rows.length;

      if (options.relatedtable && options.table == 'studiousers') {
        // check if foreign key exists 
        // select all nested table from pg in array 
        const selectqueryTemplate = 'SELECT * FROM %I ;';
        return executeQuery(client, format(selectqueryTemplate, options.relatedtable)).then((rslt) => {
          var relatedkeys = _.pluck(rslt.rows, [options.foreigkeys]);
          let filteredrows = [];
          _.each(rows, (tblrow) => {
            var row = _.clone(tblrow);
            delete row[options.relatedtable];
            // replace non existing value of userKey to null
            //console.log('row.studioKey' , row['studioKey']);
            // console.log('row.settings.studio_owner' , row.settings.studio_owner);
            if (_.has(row, 'invited_by') && !_.contains(relatedkeys, row['invited_by'])) {
              row['invited_by'] = (row.settings && _.contains(relatedkeys, row.settings.studio_owner)) ? row.settings.studio_owner : null;
            }
            if (_.has(row, 'approved_by') && !_.contains(relatedkeys, row['approved_by'])) {
              row['approved_by'] = (row.settings && _.contains(relatedkeys, row.settings.studio_owner)) ? row.settings.studio_owner : null;
            }
            if (_.has(row, options.foreigkeys) && !_.contains(relatedkeys, row['userKey'])) {
              row['userKey'] = (row.settings && _.contains(relatedkeys, row.settings.studio_owner)) ? row.settings.studio_owner : null;
            }
            filteredrows.push(row);

          })
          //  var filteredrows = _.reject(rows, function(row){ 
          //      // delete users node 
          //     delete row[options.relatedtable];        
          //     //return ((_.has(row, options.foreigkeys) && !_.contains(relatedkeys, row[options.foreigkeys])) || (_.has(row, 'invited_by')  && !_.contains(relatedkeys, row['invited_by'])) || (_.has(row, 'approved_by')  && !_.contains(relatedkeys, row['approved_by']))) ? true : false;
          //     if(_.has(row, 'invited_by')  && !_.contains(relatedkeys, row['invited_by'])){
          //       row['invited_by'] =  (row.settings.studio_owner && _.contains(relatedkeys, row.settings.studio_owner))? row.settings.studio_owner : null ;
          //      // row['invited_by'] = row.settings.studio_owner ? row.settings.studio_owner : null;
          //     }
          //     if(_.has(row, 'approved_by')  && !_.contains(relatedkeys, row['approved_by'])){
          //       //row['approved_by'] =row.settings.studio_owner ? row.settings.studio_owner : null;
          //       row['approved_by'] =  (row.settings.studio_owner && _.contains(relatedkeys, row.settings.studio_owner))? row.settings.studio_owner : null ;
          //     }
          //     return (_.has(row, options.foreigkeys) && !_.contains(relatedkeys, row['userKey']))  ? true : false;                 
          //   });
          rowCount = filteredrows.length;
          return filteredrows;
        });
      }
      return rows;
    })
    .then((rows) => {

      // if(options.table == 'studioUsers' || options.table == 'photos' || options.table == 'videos' || options.table == 'audios' || options.table == 'posts' ){
      if (_.contains(['studiousers', 'photos', 'videos', 'audios', 'posts', 'events', 'classes'], options.table)) {
        // check for studios key too
        const selectqueryTemplate = 'SELECT * FROM %I ;';
        return executeQuery(client, format(selectqueryTemplate, 'studios')).then((rslt) => {
          var relatedkeys = _.pluck(rslt.rows, 'studioKey');

          let filteredrows = [];

          _.each(rows, (tblrow) => {
            var row = _.clone(tblrow);
            row['studio'] == '' ? null : row['studio'];
            if (options.table == 'studiousers') {
              if (_.has(row, 'studioKey') && row['studioKey'] != null && !_.contains(relatedkeys, row['studioKey']))
                row['studioKey'] = null;
            } else {
              if (_.has(row, 'studio') && row['studio'] != null && !_.contains(relatedkeys, row['studio']))
                row['studio'] = null;
            }
            // }
            filteredrows.push(row);
          });
          //  var filteredrows = _.reject(rows, function(row){
          //    if(options.table == 'studiousers'){
          //     if(row['studioKey'] == '') row['studioKey']= null ;
          //     return (_.has(row, 'studioKey') && row['studioKey'] != null && !_.contains(relatedkeys, row['studioKey']))  ? true : false;  
          //    }
          //    else{
          //     if(row['studio'] == '') row['studio']= null ;
          //    return (_.has(row, 'studio') && row['studio'] != null && !_.contains(relatedkeys, row['studio']))  ? true : false;  
          //    }
          //  });
          rowCount = filteredrows.length;
          return filteredrows;
        })
      }
      return rows;
    })
    .then((rows) => {
      if (options.table == 'postcomments' || options.table == 'postapplauses') {
        // check for posts key too
        const selectqueryTemplate = 'SELECT * FROM %I ;';
        return executeQuery(client, format(selectqueryTemplate, 'posts')).then((rslt) => {
          var relatedkeys = _.pluck(rslt.rows, 'postKey');
          var filteredrows = [];
          _.each(rows, (tblrow) => {
            var row = _.clone(tblrow);
            if (row['postKey'] == '') row['postKey'] = null;
            if ((_.has(row, 'postKey') && !_.contains(relatedkeys, row['postKey']))) {
              row['postKey'] = null;
            }
            filteredrows.push(row);
          })


          rowCount = filteredrows.length;
          return filteredrows;
        })
      }

      if (options.table == 'events') {
        // check for posts key too
        const selectqueryTemplate = 'SELECT * FROM %I ;';
        return executeQuery(client, format(selectqueryTemplate, 'posts')).then((rslt) => {
          var relatedkeys = _.pluck(rslt.rows, 'postKey');
          var filteredrows = [];
          _.each(rows, (tblrow) => {
            var row = _.clone(tblrow);
            if (row['post'] == '') row['post'] = null;
            if ((_.has(row, 'post') && !_.contains(relatedkeys, row['post']))) {
              row['post'] = null;
            }
            filteredrows.push(row);
          })


          rowCount = filteredrows.length;
          return filteredrows;
        })
      }


      return rows;
    })
    .then((rows) => {
      console.info('Inserting data...');
      return Promise.all(rows.map((row) => {
        let cols = fieldsArr.length > 0 ? fieldsArr : Object.keys(row);
        // get values of columns in json file 
        let values = cols.map((col) => {
          if ((options.table == 'users' || options.table == 'studiousers') && col == 'createdAt') {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            if (valu.length == 0)
              return toUTC('1970-01-01');
          } else if ((options.table == 'users' || options.table == 'studios') && col == 'updatedAt') {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            if (valu.length == 0)
              return toUTC(new Date());
          }

          if (options.table == 'studios' && col == 'created') {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            if (valu[0]) {
              var checkdate = isNaN(valu[0]) ? new Date(valu[0]) : new Date(parseInt(valu[0]));
              if (checkdate.getFullYear() == 1970) {
                return toUTC(new Date(parseInt(valu[0]) * 1000));
              } else
                return toUTC(checkdate);
            } else
              return toUTC(new Date())

            // if (valu[0]) {
            //   var checkdate = isNaN(valu[0]) ? new Date(valu[0]) : new Date(parseInt(valu[0]));
            //   if (checkdate.getFullYear() == 1970) {
            //     return new Date(parseInt(valu[0]) * 1000).toISOString();
            //   } else
            //     return checkdate.toISOString();
            // } else
            //   return new Date().toISOString()

          }

          if ((options.table == 'events' || options.table == 'classes') && (col == 'startDate' || col == 'endDate' || col == 'startTime' || col == 'endTime')) {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            if (valu[0]) {
              var checkdate = isNaN(valu[0]) ? new Date(valu[0]) : new Date(parseInt(valu[0]));
              if (checkdate.getFullYear() == 1970) {
                return toUTC(new Date(parseInt(valu[0]) * 1000));
              } else
                // return Date.UTC(checkdate);
                return toUTC(checkdate);
            } else
              return null;
          }

          if (options.table == 'posts' && col == 'created_at') {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            let createdate = valu.length > 0 ? valu[0] ? new Date(valu[0]) : new Date() : new Date();
            return toUTC(createdate);
          }
          if (options.table == 'posts' && col == 'updated_at') {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            let updatedate = valu.length > 0 ? valu[0] ? new Date(Number(valu[0].toString().replace('-', ''))) : new Date() : new Date();
            return toUTC(updatedate);
          }
          if (options.table == 'studentclasses' && col == 'updatedAt') {
            let valu = jsonPath({
              json: row,
              path: "$..studenttimestamp"
            });
            let timestampdate = valu.length > 0 ? valu[0] ? new Date(valu[0]) : new Date() : new Date();
            return toUTC(timestampdate);
          }

          if ((_.contains(['studiousers', 'photos', 'videos', 'audios', 'postcomments', 'postapplauses', 'classes'], options.table)) && col == 'updatedAt') {
            //if((options.table == 'photos' || options.table == 'videos' || options.table == 'audios' ||  options.table == 'postcomments' ||  options.table == 'postapplauses') && col =='updatedAt'){
            let valu = jsonPath({
              json: row,
              path: "$..timestamp"
            });
            let timestampdate = valu.length > 0 ? valu[0] ? new Date(valu[0]) : new Date() : new Date();
            return toUTC(timestampdate);

          } else {
            let valu = jsonPath({
              json: row,
              path: "$.." + col,
              wrap: true
            });
            // if valuearray length in path >0 , check if empty , return null (foriegkey constraint not allow empty values)
            return valu.length > 0 ? valu[0] ? valu[0].toString().replace(',', '\,') : null : null;
          }
        });

        if (options.table == 'events') {
          if (values[5] != null)
            values[5] = combineDateWithTime(values[5], values[6])
          else
            values[5] = null;


          if (values[3] != null)
            values[2] = combineDateWithTime(values[2], values[3])
          else {
            var d = new Date()
            d.setHours(0, 0, 0, 0);
            values[2] = combineDateWithTime(values[2], d)
          }

          values.splice(3, 1)
          values.splice(5, 1)

          // console.log(JSON.stringify(values) , 'values')

        }
        // check if there is mapped fields

        let dbcols = [];
        if (options.databasefields) {
          cols.map(function (dbcol, i) {
            let dbcolumn = options.databasefields.split(',')[i];
            if (dbcolumn)
              dbcols.push(dbcolumn);
          });
        } else
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

  function combineDateWithTime(d, t) {
    return moment.utc(d).set('hour', moment.utc(t).hours()).set('minute', moment.utc(t).minutes()).set('second', moment.utc(t).seconds()).format('YYYY-MM-DD HH:mm:ss');
    // return moment.utc(d).format('YYYY-MM-DD HH:mm:ss');

  }

  function toUTC(d) {

    return moment.utc(d).format('YYYY-MM-DD HH:mm:ss');

  }


  var pluckMany = function () {
    // get the property names to pluck
    var source = arguments[0];
    var propertiesToPluck = _.rest(arguments, 1);
    return _.map(source, function (item) {
      var obj = {};
      _.each(propertiesToPluck, function (property) {
        obj[property] = item[property];
      });
      return obj;
    });
  };

});