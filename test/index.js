'use strict';

var resolve   = require('path').resolve
  , rmdir     = require('fs2/rmdir')
  , Database  = require('dbjs')
  , dbjsLevel = require('dbjs-level')

  , dbPath = resolve(__dirname, 'test-db')
  , mapEvents = function (event) { return event.object.__valueId__; };

require('levelup');

module.exports = function (T, a, d) {
	var db = new Database()
	  , persistentDb = dbjsLevel(db, { path: dbPath })
	  , cluster = new T(persistentDb)

	  , obj1 = new db.Object({ firstName: 'Mark', lastName: 'Smith', age: 20 })
	  , obj2 = new db.Object({ firstName: 'Eve', lastName: 'Nowak', age: 30 })
	  , obj3 = new db.Object({ firstName: 'Peter', lastName: 'Smith', age: 10 })
	  , obj4 = new db.Object({ firstName: 'John', lastName: 'Albin', age: 45 });

	cluster.persistentDb.close()(function () {
		var db = new Database()
		  , cluster = new T(dbjsLevel(db, { path: dbPath }));

		return cluster.getObjectData(obj1.__id__)(function (events) {
			a.deep(events.map(mapEvents), [obj1.__id__, obj1.__id__ + '/firstName',
				obj1.__id__ + '/lastName', obj1.__id__ + '/age'].sort());
		})(function () {
			return cluster.getObjectsSelectedData([obj1.__id__, obj2.__id__, obj3.__id__], {
				firstName: true,
				age: true
			})(function (events) {
				a.deep(events.map(mapEvents), [
					obj1.__id__,
					obj1.__id__ + '/firstName',
					obj1.__id__ + '/age',
					obj2.__id__,
					obj2.__id__ + '/firstName',
					obj2.__id__ + '/age',
					obj3.__id__,
					obj3.__id__ + '/firstName',
					obj3.__id__ + '/age'
				].sort());
			});
		})(function () {
			return cluster.getCollectionView('smith', db.Object.instances,
				'lastName', 'Smith', 'age')(function (items) {
				a.deep(items, [
					{ id: obj3.__id__, sortIndex: 10 },
					{ id: obj1.__id__, sortIndex: 20 }
				]);
			});
		})(function () {
			return cluster.getCollectionView('smith:lastmod', db.Object.instances,
				'lastName', 'Smith', 'age:lastModified')(function (items) {
				a.deep(items, [
					{ id: obj1.__id__, sortIndex: items[0].sortIndex },
					{ id: obj3.__id__, sortIndex: items[1].sortIndex }
				]);
			});
		})(function () {
			return cluster.searchCollectionView('smith', function (obj) {
				return obj.firstName === 'Peter';
			})(function (items) {
				a.deep(items, [
					{ id: obj3.__id__, sortIndex: 10 }
				]);
			});
		})(function () {
			return cluster.getCollectionSize('age>20', db.Object.instances,
				'age', function (val) { return val > 20; }, 'age')(function (data) {
				a.deep(data, { value: 2, stamp: data.stamp });
			});
		})(function () {
			return cluster.getObjectData(obj4.__id__)(function (events) {
				a.deep(events.map(mapEvents), [obj4.__id__, obj4.__id__ + '/firstName',
					obj4.__id__ + '/lastName', obj4.__id__ + '/age'].sort());
			});
		})(function () {
			return persistentDb.close()(function () {
				rmdir(dbPath, { recursive: true, force: true });
			});
		});
	}).done(function () { d(); }, d);
};
