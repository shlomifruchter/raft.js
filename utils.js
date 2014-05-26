var config = require('./config');

exports.randomInt = function (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
};

exports.setupHosts = function() {
	var servers = {};
	for(var i = 1; i <= config.numServers; i++) {
		servers[i] = {
			host: config.host,
			port: config.portRangeStart + i
		};
	}

	return servers;
};

exports.assert = function(condition, msg) {
	if(!condition) {
		throw 'assertion failed: ' + msg;
	}
};

exports.forEachRemoteServer = function(servers, currentServerId, callback) {
	for(var id in servers) {
		var currentId = parseInt(id);
		// Skip self
		if(currentId === currentServerId) {
			continue;
		}

		callback(currentId);
	}
};