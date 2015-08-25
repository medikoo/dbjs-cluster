'use strict';

var resolve = require('path').resolve

  , fork = require('child_process').fork;

module.exports = function (t, a, d) {
	var log = { arraySlice: 0, dbBatch: 0, setSize: 0 }
	  , childProcess = fork(resolve(__dirname, '__playground/cluster-process.js'),
		{ cwd: process.cwd(), env: process.env, silent: false });

	childProcess.on('error', function (err) { d(err); });

	childProcess.on('message', function (message) {
		++log[message.type];
	});
	childProcess.on('close', function () {
		a.deep(log, { arraySlice: 2, dbBatch: 1, setSize: 1 });
		d();
	});
};
