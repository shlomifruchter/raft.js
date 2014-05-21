var HttpServer = require('./HttpServer');
var Logger = require('./Logger');
var Raft = require('./Raft');

GLOBAL.config = {
	numServers: 2
};
var host = 'localhost';
var portPrefix = '777';

// Setup server hosts
GLOBAL.servers = {};
for(var i = 1; i <= GLOBAL.config.numServers; i++) {
	GLOBAL.servers[i] = {
		host: host,
		port: portPrefix + i
	};
}

var role = process.argv[2];
var serverId = process.argv[3];
var port = portPrefix + serverId;

// Setup logger
GLOBAL.logger = new Logger({
	deferLogging: role != 'logger',
	logServerHost: 'localhost',
	logServerPort: '7770',
	serverId: serverId
});

if(role != 'logger') {
	// Setup Raft
	var raft = new Raft({
		serverId: serverId,
		host: host,
		port: port,
		electionTimeoutInterval: 2000,
		heartbeatTimeoutInterval: 1000
	});

	raft.start();

	logger.log('Raft server running at http://' + host + '/' + port);
}