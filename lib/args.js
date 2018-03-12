const fs = require('fs');

function File(filename) {
  if (!(this instanceof File)) return new File(filename);
  this.filename = filename;
 
  this.exists = fs.existsSync(filename);
}

module.exports = [
  {
    name: 'file',
    alias: 'f',
    type: File,
    description: 'The JSON file to load'
  },
  {
    name: 'connection',
    alias: 'c',
    description: 'A connection string to PostgreSQL. i.e. postgresql://localhost:5432/mydb'
  },
  {
    name: 'table',
    alias: 't',
    description: 'The table name where data should be imported'
  },
  {
    name: 'node',
    alias: 'n',
    description: 'json node coressponding to table'
  },
  {
    name: 'fields',
    alias: 'F',
    description: 'Custom fields to import'
  },
  {
    name: 'key',
    alias: 'k',
    description: 'name of primary key for the imported table'
  },
  {
    name: 'help',
    description: 'show this message'
  }
];
