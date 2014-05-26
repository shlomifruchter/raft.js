var http = require('http');

var HTTP_SUCCESS = 200;

module.exports = function(raft) {
	_this = this;
	this.raft = raft;
	this.methods = {};

	function log(msg) {
		if(GLOBAL.logger) {
			GLOBAL.logger.log('(HttpServer) ' + msg);
		}
	}

	function handleRPC(method, params, success, error) {
		if(_this.methods[method] instanceof Function) {
			try {
				_this.methods[method](params, success, error);
			} catch (e) {
				error({
					message: 'failed to handle RPC: ' + method + ' , exception: ' + e
				});
				return;
			}
		} else {
			error({
				message: 'undefined RPC: ' + method
			});
			return;
		}
	}

	return {
		create: function (host, port) {
			_this.httpServer = http.createServer(function(req, res) {
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
						log('failed to handle RPC: ' + e);
						error(e);
					}
				});
			}).listen(port, host);
		},
		invokeRPC: function(remoteServerId, method, params, success, error) {
			var payload = JSON.stringify({
				method: method,
				params: params
			});

			var options = {
				host: GLOBAL.servers[remoteServerId].host,
				port: GLOBAL.servers[remoteServerId].port,
				path: '/',
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'Content-Length': Buffer.byteLength(payload),
				}
			};

			var req = http.request(options, function(res) {
				var callback = res.statusCode === HTTP_SUCCESS ? success : error;

				res.setEncoding('utf-8');
				var payload = '';
				res.on('data', function(chunk) {
					payload += chunk;
				});
				res.on('end', function() {
					var parsedPayload = null;
					try {
						parsedPayload = JSON.parse(payload);
					} catch(e) {
						log('invokeRPC for ' + method + ': parsing of payload failed: ' + payload);
						error(e);
						return;
					}

					try {
						success(parsedPayload);
					} catch(e) {
						log('invokeRPC for ' + method + ': success callback threw an exception: ' + e);
						error(e);
						return;
					}
				});
			});

			req.on('error', function(e) {
				log('error in invokeRPC: ' + e.message + ', options: ' + JSON.stringify(options));
				error(e);
			});

			req.write(payload);
			req.end();
		},
		regsiterMethod: function(methodName, callback) {
			_this.methods[methodName] = callback;
		}
	};
};