var http = require('http');
var request = require('request');

module.exports = function(_raft) {
	var raft = _raft;

	return {
		create: function (host, port) {

			http.createServer(function(req, res) {
				var success = function(responseObject) {
					responseBody = JSON.stringify(responseObject);
					res.writeHead(200, {
						'Content-Type': 'application/json',
						'Content-Length': Buffer.byteLength(responseBody),
					});
					res.write(responseBody);
					res.end();
				};
				var error = function(responseObject) {
					responseBody = JSON.stringify(responseObject);
					res.writeHead(500, {
						'Content-Type': 'application/json',
						'Content-Length': Buffer.byteLength(responseBody),
					});
					res.write(responseBody);
					res.end();
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
		},
		handleRPC: function(method, params, success, error) {
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
		},
		invokeRPC: function(remoteServerId, method, params, success, error) {
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
					'Content-Length': Buffer.byteLength(payload)
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
	};
};