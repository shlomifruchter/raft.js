var HttpServer = require('./HttpServer');
var Logger = require('./Logger');
var Raft = require('./Raft');
var utils = require('./utils');

GLOBAL.config = {
	numServers: 5
};
var host = 'localhost';
var portRangeStart = 8080;

// Setup server hosts
GLOBAL.servers = {};
for(var i = 1; i <= GLOBAL.config.numServers; i++) {
	GLOBAL.servers[i] = {
		host: host,
		port: portRangeStart + i
	};
}

var role = process.argv[2];
var serverId = parseInt(process.argv[3]); // Important: server ids are always integers, not strings!
var port = portRangeStart + serverId;

// Setup logger
GLOBAL.logger = new Logger({
	listenForRemoteLogs: role === 'logger',
	deferLogging: false, // role != 'logger',
	logServerHost: 'localhost',
	logServerPort: 7770,
	serverId: serverId
});

var raft = null;

if(role != 'logger') {
	// Setup Raft
	raft = new Raft({
		serverId: serverId,
		host: host,
		port: port,
		electionTimeoutInterval: utils.randomInt(2000, 2300),
		heartbeatTimeoutInterval: 500
	});

	raft.start();

	logger.log('Raft server running at http://' + host + ':' + port);
}