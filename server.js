var http = require('http');
var Logger = require('./Logger');
var Raft = require('./Raft');

GLOBAL.config = {
	numServers: 5
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

var raft = null;

if(role != 'logger') {
	// Setup Raft
	raft = new Raft({
		serverId: serverId,
		host: host,
		port: port
	});

	raft.on('invokeRPC', function(remoteServerId, method, params, success, error) {
		invokeRPC(remoteServerId, method, params, success, error);
	});

	setupHttpServer();

	logger.log('Raft server running at http://' + host + '/' + port);
}

function setupHttpServer() {
	http.createServer(function(req, res) {
		var success = function() {
			res.writeHead(200, {
				'Content-Type': 'application/json'
			});
			res.end('RPC logged');
		};
		var error = function() {

		};
		req.setEncoding('utf8');
		var payload = '';
		req.on('data', function(chunk) {
			payload += chunk;
		});
		req.on('end', function() {
			try {
				var rpcRequestObject = JSON.parse(payload);
				handleRPC(rpcRequestObject.method, rpcRequestObject.params, success, error);
			} catch(e) {
				error(e);
			}
		});
	}).listen(port, host);
}

function handleRPC(method, params, success, error) {
	if(!raft) {
		error({
				message: 'Raft is not initialized'
		});
	}

	switch(method) {
		case 'RequestVote':
			raft.requestVote(params, success, error);
			break;
		case 'followerAppendEntries':
			raft.followerAppendEntries(params, success, error);
			break;
		case 'appendEntry':
			raft.appendEntry(params, success, error);
			break;
		default:
			error({
				message: 'undefined RPC: ' + method
			});
	}
}

function invokeRPC(remoteServerId, method, params, success, error) {
	var payload = JSON.stringify({
		method: method,
		params: params
	});

	var req = http.request({
		host: GLOBAL.servers[remoteServerId].host,
		port: GLOBAL.servers[remoteServerId].port,
		path: '/',
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'Content-Length': payload.length
		}
	}, function(res) {
		res.setEncoding('utf-8');
		var payload = '';
		res.on('data', function(chunk) {
			payload += chunk;
		});
		res.on('end', function() {
			try {
				success(JSON.parse(payload));
			} catch(e) {
				error(e);
			}
		});
	});

	req.on('error', function(e) {
		error(e);
	});

	req.write(payload);
	req.end();
}

// EO HTTP RPC