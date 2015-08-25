'use strict';

var resolve        = require('path').resolve
  , rmdir          = require('fs2/rmdir')
  , Database       = require('dbjs')
  , dbjsLevel      = require('dbjs-level')
  , Cluster        = require('../../')
  , clusterProcess = require('../../process')

  , dbPath = resolve(__dirname, 'test-db-process');

require('levelup');

var db = new Database()
  , persistentDb = dbjsLevel(db, { path: dbPath }), cluster;

db.Object({ firstName: 'Mark', lastName: 'Smith', age: 20 });
db.Object({ firstName: 'Eve', lastName: 'Nowak', age: 30 });
db.Object({ firstName: 'Peter', lastName: 'Smith', age: 10 });

cluster = clusterProcess(new Cluster(persistentDb));

db.Object({ firstName: 'John', lastName: 'Albin', age: 45 });

cluster.initializeSet('smith', db.Object.instances.filterByKeyPath('lastName', 'Smith'));
cluster.initializeArray('smithByAge', 'smith', 'age');

cluster.requestArraySlice('smithByAge').done(function (result) { result.emit(); });

cluster.initializeArray('smithByAgeLastModified', 'smith', 'age:lastModified');
cluster.requestArraySlice('smithByAgeLastModified').done(function (result) { result.emit(); });

cluster.initializeSet('age>20', db.Object.instances.filterByKeyPath('age',
	function (val) { return val > 20; }));
cluster.requestSetSize('age>20').done(function (result) { result.emit(); });

setTimeout(function () {
	return persistentDb.close()(function () {
		return rmdir(dbPath, { recursive: true, force: true });
	}).done();
}, 300);
