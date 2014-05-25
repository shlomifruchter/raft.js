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
}