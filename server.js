var HttpServer = require('./HttpServer');
var Logger = require('./Logger');
var Raft = require('./Raft');
var utils = require('./utils');
var config = require('./config');

// Setup server hosts
GLOBAL.servers = utils.setupHosts();

var role = process.argv[2];
var serverId = parseInt(process.argv[3]); // Important: server ids are always integers, not strings!
var port = config.portRangeStart + serverId;

// Setup logger
GLOBAL.logger = new Logger({
	listenForRemoteLogs: role === 'logger',
	deferLogging: false, // role != 'logger',
	logServerHost: config.host,
	logServerPort: 7770,
	serverId: serverId
});

var raft = null;

if(role != 'logger') {
	// Setup Raft
	raft = new Raft({
		serverId: serverId,
		host: config.host,
		port: port,
		electionTimeoutInterval: utils.randomInt(2000, 2300),
		heartbeatTimeoutInterval: 500
	});

	raft.start();

	logger.log('Raft server running at http://' + config.host + ':' + port);
}