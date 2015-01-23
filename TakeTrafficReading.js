#!/usr/bin/env node

'use strict';

/**
 *  Called by cron.  Takes one traffic reading and stores in mongo
 */


var _ = require('underscore');
var rest = require('node-rest-client').Client;
var moment = require('moment');
var winston = require('winston');
var restClient = new rest();
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var argv = require('optimist').argv;
var Q = require('Q');

var colorLogger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({colorize: true, level: 'debug'})
    ]
});

var url = 'mongodb://localhost:27017/traffic70';

// Use connect method to connect to the Server
var db;
var rawTrafficCol;
var summaryCol;

MongoClient.connect(url, function (err, _db) {
    assert.equal(null, err);
    colorLogger.log('debug', 'Connected to mongo');
    db = _db;

    rawTrafficCol = db.collection('rawTraffic');
    summaryCol = db.collection('summary');
});


module.exports = (function () {

    var getTrafficData = function () {
        var defRestCall = Q.defer();
        restClient.get('http://www.cotrip.org/speed/getSegments.do', function (rawData, response) {
            defRestCall.resolve(rawData);
        }).on('error', function (err) {
            defRestCall.reject(err);
        });
        return defRestCall.promise;
    };

    var takeReading = function () {
        var defer = Q.defer();

        colorLogger.log('debug', 'Requesting traffic data from codot...');

        getTrafficData()
            .then(function (rawData) {
                var data = JSON.parse(rawData);

                colorLogger.log('debug', 'inserting raw record in mongo');

                return Q.ninvoke(rawTrafficCol, 'insert', data)
                    .then(function () {
                        colorLogger.log('info', 'inserted raw record');

                        var westSegments = _.filter(data.SpeedDetails.Segment, function (seg) {
                            return seg.RoadName.toLowerCase() === "i-70" && seg.Direction.toLowerCase() === "west";
                        });

                        var westSegmentsCurrent = _.filter(westSegments, function (seg) {
                            // we only want samples that were calculated in the last 5 minutes
                            return Math.abs(moment(seg.calculatedData).diff(moment(), 'seconds')) < 60 * 5;
                        });

                        if (westSegments.length !== westSegmentsCurrent.length) {
                            colorLogger.log('warn', 'WARNING, there were ', westSegments.length - westSegmentsCurrent.length, ' WESTBOUND records that were older than 5 min and thus unusable');
                        }

                        var eastSegments = _.filter(data.SpeedDetails.Segment, function (seg) {
                            return seg.RoadName.toLowerCase() === "i-70" && seg.Direction.toLowerCase() === "east";
                        });

                        var eastSegmentsCurrent = _.filter(eastSegments, function (seg) {
                            return Math.abs(moment(seg.calculatedData).diff(moment(), 'seconds')) < 60 * 5;
                        });

                        if (eastSegments.length !== eastSegmentsCurrent.length) {
                            colorLogger.log('warn', 'WARNING, there were ', eastSegments.length - eastSegmentsCurrent.length, ' EASTBOUND records that were older than 5 mins and thus unusable');
                        }

                        var westTotalTravelTimeSec = _.reduce(westSegmentsCurrent, function (init, seg) {
                                return init + parseInt(seg.TravelTimeInSeconds);
                            },
                            0);

                        var eastTotalTravelTimeSec = _.reduce(eastSegmentsCurrent, function (init, seg) {
                                return init + parseInt(seg.TravelTimeInSeconds);
                            },
                            0);

                        colorLogger.log('info', 'west bound total travel time ', westTotalTravelTimeSec / 60, ' minutes');
                        colorLogger.log('info', 'east bound total travel time ', eastTotalTravelTimeSec / 60, ' minutes');

                        colorLogger.log('info', 'inserting summary record');

                        var summaryRecord = {
                            'westTotalTravelTimeSec': westTotalTravelTimeSec,
                            'eastTotalTravelTimeSec': eastTotalTravelTimeSec,
                            'dateTime': moment().toDate()
                        };

                        return Q.ninvoke(summaryCol, 'insert', summaryRecord);
                    })
                    .done(function () {
                        defer.resolve();
                    })
                    .fail(function (err) {
                        colorLogger.log('error', err);
                        defer.reject(err);
                    });

            });
        return defer.promise;
    };


    var scheduleReading = function (ms, once) {

        setTimeout(function () {
            var result = takeReading();
            if (result === false) {
                colorLogger.log('warn', 'failed to take reading, so retrying in 30s');
            } else {
                if (once) {

                    colorLogger.log('info', 'closing DB connection');
                    setTimeout(function () {
                        db.close();
                    }, 5000);

                } else {
                    colorLogger.log('debug', 'rescheduling');
                    scheduleReading(1000 * 30);
                }
            }
        }, ms || 1000 * 60 * 5);
    };

    colorLogger.log('debug', 'Starting...');

    if (argv.help) {
        colorLogger.log('debug', 'Usage:');
        colorLogger.log('debug', 'TakeTrafficReading [--schedule sec] # take a reading ever sec seconds');
        process.exit(0);
    }

    if (argv.schedule) {
        colorLogger.log('info', 'taking reading every', argv.schedule, 'seconds');
        scheduleReading(argv.schedule * 1000, false);
    } else {
        colorLogger.log('info', 'running once');
        takeReading()
            .then(function () {
                colorLogger.log('info', 'all done, closing db');
                db.close();
            })
            .fail(function (err) {
                colorLogger.log('error', err);
            });
    }
})();






