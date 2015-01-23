#!/usr/bin/env node

'use strict';

/**
 *  Called by cron.  Takes one traffic reading and stores in mongo
 */

var winston = require('winston');
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var Q = require('Q');
var rest = require('node-rest-client').Client;
var restClient = new rest();

var colorLogger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({colorize: true, level: 'debug'})
    ]
});

var url = 'mongodb://localhost:27017/traffic70';

var db;
var rawTrafficCol;
var summaryCol;


colorLogger.log('debug', 'before connect');


var getTrafficData = function () {
    var promiseRestCall = Q.defer();
    restClient.get('http://www.cotrip.org/speed/getSegments.do', function (rawData, response) {
        promiseRestCall.resolve(rawData);

    }).on('error', function (err) {
        promiseRestCall.reject(err);
    });
    return promiseRestCall.promise;
};

getTrafficData()
    .then(function (rawData) {
        colorLogger.log('debug', 'got back rawData', rawData);
        var parsedData = JSON.parse(rawData);

        return Q.nfcall(MongoClient.connect, url)
            .then(function (_db) {
                db = _db;
                colorLogger.log('debug', 'connected, now getting collections');
                rawTrafficCol = db.collection('rawTraffic');
                summaryCol = db.collection('summary');

                var someRecord = {
                    'a': 1,
                    'b': 2
                };

                colorLogger.log('debug', 'inserting one');
                return Q.ninvoke(summaryCol, 'insert', someRecord);
            })
            .then(function (res) {
                colorLogger.log('debug', 'inserting two');
                var someRecord = {
                    'a': 1,
                    'b': 2
                };
                return Q.ninvoke(summaryCol, 'insert', someRecord);
            })
            .then(function (res) {
                colorLogger.log('debug', 'inserting three');
                var someRecord = {
                    'a': 1,
                    'b': 2
                };
                return Q.ninvoke(summaryCol, 'insert', someRecord);
            })
            .done(function () {
                colorLogger.log('info', 'all done, closing db');
                db.close();
            });
    });


