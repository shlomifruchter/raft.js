var HttpServer = require('../HttpServer');
var Logger = require('../Logger');
var utils = require('../utils');
var config = require('../config');

// Setup server hosts
GLOBAL.servers = utils.setupHosts();

var httpServer = new HttpServer();

process.stdin.setEncoding('utf8');

// Simple command line client

process.stdout.write('Send log entries to Raft cluster using the following format: "SERVER_ID:ENTRY"\n');
process.stdout.write('For example:\n');
process.stdout.write('1:this is my entry\n');
process.stdout.write('\n');

process.stdin.on('readable', function() {
	var line = process.stdin.read();

	if (line == null) {
		return;
	}
	
	// Parse line
	var line = line.replace(/\r\n/g, ''); // remove newline
	var colonIndex = line.indexOf(':');
	if(colonIndex < 0 ) {
		return;
	}

	var serverId = null,
		entry = null;
	try{ 
		serverId = parseInt(line.substr(0, colonIndex));
		entry = line.substr(colonIndex + 1, line.length - colonIndex - 1);
	} catch (e) {
		process.stdout.write('error in parsing line: ' + e + '\n');
		return;
	}

	process.stdout.write('append entry "' + entry + '" to server ' + serverId + '\n');

	appendEntry(serverId, entry);
});

function appendEntry(serverId, entry, redirectDepth) {
	if(!redirectDepth) {
		redirectDepth = 0;
	}

	if(redirectDepth > config.maxRedirectDepth) {
		process.stdout.write('max redirect depth reached\n');
		return;
	}

	httpServer.invokeRPC(serverId, 'AppendEntry', {
		entry: entry 
	}, function(response) { // success
		process.stdout.write('server ' + serverId + ' responded: ' + JSON.stringify(response) + '\n');

		// Follow redirect
		if(response.redirect) {
			appendEntry(response.redirect, entry, redirectDepth+1);
		}
	}, function(response) { // error
		process.stdout.write('error invoking AppendEntry: ' + JSON.stringify(response) + '\n');
	});
}