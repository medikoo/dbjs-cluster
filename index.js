'use strict';

var toNatural           = require('es5-ext/number/to-pos-integer')
  , assign              = require('es5-ext/object/assign')
  , ensureString        = require('es5-ext/object/validate-stringifiable-value')
  , endsWith            = require('es5-ext/string/#/ends-with')
  , d                   = require('d')
  , lazy                = require('d/lazy')
  , ee                  = require('event-emitter')
  , deferred            = require('deferred')
  , memoizeMethods      = require('memoizee/methods')
  , getStamp            = require('time-uuid/time')
  , ensureObservableSet = require('observable-set/valid-observable-set')
  , tokenizeKeyPath     = require('dbjs/_setup/utils/resolve-property-path').tokenize

  , push = Array.prototype.push, create = Object.create, defineProperty = Object.defineProperty
  , stringify = JSON.stringify;

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

var getSliceSnapshot = function (slice, array) {
	var result = [], resolve = array.dbjsCompare$settings.resolve
	  , keyTokens = array.dbjsCompare$settings.keyTokens;
	slice.forEach(function (obj) {
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
	searchArray: d(function (arrayName, filter, start, end) {
		var array = this._observableArrays[arrayName], result, index, resolve, keyTokens;
		if (!array) throw new Error("Array for " + stringify(arrayName) + " was not initialized yet");
		start = toNatural(start);
		end = toNatural(end) || Infinity;
		resolve = array.dbjsCompare$settings.resolve;
		keyTokens = array.dbjsCompare$settings.keyTokens;
		result = [];
		if (end <= start) return result;
		index = -1;
		array.some(function (obj) {
			if (!filter(obj)) return;
			++index;
			if (index < start) return;
			if (index === end) return true;
			result.push({ id: obj.__id__, sortIndex: resolve(obj, keyTokens) });
		}, this);
		return deferred(result);
	}),
	initializeSet: d(function (name, set) {
		if (this._observableSets[name]) {
			throw new Error("Set for " + stringify(name) + " is already initialized");
		}
		return (this._observableSets[name] = ensureObservableSet(set));
	}),
	initializeArray: d(function (arrayName, setName, sortKeyPath) {
		var compare, keyTokens, resolve, array;
		if (!this._observableSets[setName]) {
			throw new Error("Array for " + stringify(setName) + " was not initialized yet");
		}
		if ((arrayName == null) && (compare == null)) arrayName = setName;
		arrayName = ensureString(arrayName);
		if (this._observableArrays[arrayName]) {
			throw new Error("Set for " + stringify(arrayName) + " is already initialized");
		}
		sortKeyPath = ensureString(sortKeyPath);
		if (endsWith.call(sortKeyPath, ':lastModified')) {
			sortKeyPath = sortKeyPath.slice(0, -':lastModified'.length);
			resolve = resolveValueLastModified;
		} else {
			resolve = resolveValue;
		}
		keyTokens = tokenizeKeyPath(sortKeyPath);
		compare = function (a, b) { return resolve(a, keyTokens) - resolve(b, keyTokens); };
		array = this._observableArrays[arrayName] = this._observableSets[setName].toArray(compare);
		return defineProperty(array, 'dbjsCompare$settings', d('', {
			resolve: resolve,
			keyTokens: keyTokens
		}));
	})
}, lazy({
	_observableSets: d(function () { return create(null); }),
	_observableArrays: d(function () { return create(null); })
}), memoizeMethods({
	requestArraySlice: d(function (arrayName, start, end) {
		var array = this._observableArrays[arrayName], slice, emit;
		if (!array) throw new Error("Array for " + stringify(arrayName) + " was not initialized yet");
		slice = array.slice(start, end);
		slice.on('change', emit = function () {
			this.emit('arrayslice', { id: arrayName, start: start, end: end,
				value: getSliceSnapshot(slice, array) });
		}.bind(this));
		return deferred({
			get: function () { return getSliceSnapshot(slice, array); },
			emit: emit
		});
	}, { resolvers: [ensureString, toNatural, function (value) {
		return toNatural(value) || Infinity;
	}] }),
	requestSetSize: d(function (setName) {
		var set = this._observableSets[setName], emit;
		if (!set) throw new Error("Set for " + stringify(setName) + " was not initialized yet");
		return this.persistentDb.getCustom('_size:' + setName)(function (value) {
			var index, stamp;
			if (value) {
				index = value.indexOf('.');
				stamp = Number(value.slice(0, index));
				value = Number(value.slice(index + 1));
			}
			if (value !== set.size) {
				stamp = getStamp();
				value = set.size;
				this.persistentDb.storeCustom('_size:' + setName, stamp + '.' + value).done();
			}
			emit = function () {
				this.emit('setsize', { id: setName, stamp: stamp, value: value });
			}.bind(this);
			set._size.on('change', function (event) {
				stamp = getStamp();
				value = event.newValue;
				this.persistentDb.storeCustom('_size:' + setName, stamp + '.' + value).done();
				emit();
			}.bind(this));
			return {
				get: function () { return { value: value, stamp: stamp }; },
				emit: emit
			};
		}.bind(this));
	})
}))));
