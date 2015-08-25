'use strict';

var clear     = require('es5-ext/array/#/clear')
  , serialize = require('dbjs/_setup/serialize/value')
  , once      = require('timers-ext/once');

module.exports = function (cluster) {
	var eventsToEmit = [];
	var emitEvents = once(function () {
		process.send({ type: 'dbBatch', data: eventsToEmit.map(function (event) {
			return { id: event.object.__valueId__, stamp: event.stamp, value: serialize(event.value) };
		}) });
		clear.call(eventsToEmit);
	});
	cluster.on('setsize', function (event) {
		process.send({ type: 'setSize', id: event.id, stamp: event.stamp, value: event.value });
	});
	cluster.on('arrayslice', function (event) {
		process.send({ type: 'arraySlice', id: event.id, value: event.value, start: event.start,
			end: event.end });
	});
	cluster.db.objects.on('update', function (event) {
		eventsToEmit.push(event);
		emitEvents();
	});
	return cluster;
};
