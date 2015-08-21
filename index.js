'use strict';

var assign          = require('es5-ext/object/assign')
  , ensureString    = require('es5-ext/object/validate-stringifiable-value')
  , endsWith        = require('es5-ext/string/#/ends-with')
  , d               = require('d')
  , ee              = require('event-emitter')
  , deferred        = require('deferred')
  , memoizeMethods  = require('memoizee/methods-plain')
  , getStamp        = require('time-uuid/time')
  , tokenizeKeyPath = require('dbjs/_setup/utils/resolve-property-path').tokenize

  , push = Array.prototype.push;

var getBriefDataEvents = function (obj, map, events) {
	var event = obj._lastOwnEvent_, idLength = obj.master.__id__.length + 1;
	if (event) events.push(event);
	obj._forEachOwnDescriptor_(function (desc) {
		if (!desc._sKey_) return;
		if (!map[desc.__valueId__.slice(idLength)]) return;
		event = desc._lastOwnEvent_;
		if (event) events.push(event);
	});
	obj._forEachOwnItem_(function (item) {
		if (!item._pSKey_) return;
		if (!map[item.object.__id__ + '/' + item._pSKey_].slice(idLength)) return;
		event = item._lastOwnEvent_;
		if (event) events.push(event);
	});
	obj._forEachOwnNestedObject_(function (object) {
		push.apply(events, getBriefDataEvents(object, map, events));
	});
	return events;
};

var resolveValue = function (obj, keyTokens) {
	var i, sKey;
	for (i = 0; (sKey = keyTokens[i]); ++i) {
		if (obj == null) return obj;
		obj = obj._get_(sKey);
	}
	return obj;
};

var resolveValueLastModified = function (obj, keyTokens) {
	var i, sKey, l = keyTokens.length - 1;
	for (i = 0; i < l; ++i) {
		if (obj == null) return obj;
		obj = obj._get_(keyTokens[i]);
	}
	if (!obj || !obj._getPropertyLastModified_) return null;
	sKey = keyTokens[l];
	return obj._getPropertyLastModified_(sKey);
};

var getCollectionSnapshot = function (collection, resolve, keyTokens) {
	var result = [];
	collection.forEach(function (obj) {
		result.push({ id: obj.__id__, sortIndex: resolve(obj, keyTokens) });
	});
	return result;
};

var DbjsCluster = module.exports = function (persistentDb) {
	if (!(this instanceof DbjsCluster)) return new DbjsCluster(persistentDb);
	this.persistentDb = persistentDb;
	this.db = persistentDb.db;
};
ee(Object.defineProperties(DbjsCluster.prototype, assign({
	getObjectData: d(function (id) {
		var obj = this.db.objects.getById(ensureString(id));
		if (!obj) return this.persistentDb.loadObject(id);
		return deferred(obj.getAllEvents());
	}),
	getObjectsSelectedData: d(function (ids, dataMap) {
		var events = [];
		return deferred.map(ids, function (id) {
			var obj = this.db.objects.getById(id);
			if (obj) {
				getBriefDataEvents(obj, dataMap, events);
				return;
			}
			return this.persistentDb.loadObject(id)(function () {
				return getBriefDataEvents(this.db.objects.getById(id), dataMap, events);
			}.bind(this));
		}.bind(this))(events);
	}),
	getCollectionView: d(function (collectionName, initSet, keyPath, filter, sortKeyPath) {
		return this._setupCollectionView(collectionName, initSet, keyPath, filter, sortKeyPath)(
			function (data) {  return data.getResult(); }
		);
	}),
	searchCollectionView: d(function (collectionName, filter) {
		return this._setupCollectionView(collectionName)(function (data) {
			var result = [];
			data.getResult().forEach(function (item) {
				if (filter(this.db.objects.getById(item.id))) result.push(item);
			}, this);
			return result;
		}.bind(this));
	}),
	getCollectionSize: d(function (collectionName, initSet, keyPath, filter) {
		return this._setupCollectionSize(collectionName, initSet, keyPath, filter)(function (data) {
			return data.getResult();
		});
	})
}, memoizeMethods({
	_loadAll: d(function () { return this.persistentDb.loadAll(); }),
	_setupCollection: d(function (collectionName, initSet, keyPath, filter) {
		return this._loadAll()(function () {
			return initSet.filterByKeyPath(keyPath, filter);
		});
	}, { length: 1 }),
	_setupCollectionView: d(function (collectionName, initSet, keyPath, filter, sortKeyPath) {
		return this._setupCollection(collectionName, initSet, keyPath, filter)(function (collection) {
			var compare, keyTokens, resolve;
			if (endsWith.call(sortKeyPath, ':lastModified')) {
				sortKeyPath = sortKeyPath.slice(0, -':lastModified'.length);
				resolve = resolveValueLastModified;
			} else {
				resolve = resolveValue;
			}
			keyTokens = tokenizeKeyPath(sortKeyPath);
			compare = function (a, b) { return resolve(a, keyTokens) - resolve(b, keyTokens); };
			collection = collection.toArray(compare);
			collection.on('change', function () {
				this.emit('collectionview',
					{ value: getCollectionSnapshot(collection, resolve, keyTokens), id: collectionName });
			}.bind(this));
			return { getResult: function () {
				return getCollectionSnapshot(collection, resolve, keyTokens);
			}, collection: collection };
		});
	}, { length: 1 }),
	_setupCollectionSize: d(function (collectionName, initSet, keyPath, filter) {
		return this._setupCollection(collectionName, initSet, keyPath, filter)(function (collection) {
			var value, stamp;
			collection._size.on('change', function (event) {
				stamp = getStamp();
				value = event.newValue;
				this.persistentDb.storeCustom('_size:' + collectionName, stamp + '.' + value).done();
				this.emit('collectionsize', { value: value, id: collectionName, stamp: stamp });
			}.bind(this));
			return this.persistentDb.getCustom('_size:' + collectionName)(function (value) {
				var index, stamp;
				if (value) {
					index = value.indexOf('.');
					stamp = Number(value.slice(0, index));
					value = Number(value.slice(index + 1));
				}
				if (value !== collection.size) {
					stamp = getStamp();
					value = collection.size;
					this.persistentDb.storeCustom('_size:' + collectionName, stamp + '.' + value).done();
				}
				return { getResult: function () { return { value: value, stamp: stamp }; },
					collection: collection };
			}.bind(this));
		}.bind(this));
	}, { length: 1 })
}))));
