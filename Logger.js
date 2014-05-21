var http = require('http');

var Logger = function(options) {

	var options = options;
	var _this = this;

	this.log = function(msg) {
		if(options.deferLogging) {
			_this._deferLog(msg);
		} else {
			console.log(msg);
		}
	};

	this._deferLog = function(msg) {
		var messageJson = {
			msg: msg,
			serverId: options.serverId
		};

		var payload = JSON.stringify(messageJson);

		var req = http.request({
			host: options.logServerHost,
			port: options.logServerPort,
			path: '/',
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Content-Length': Buffer.byteLength(payload)
			}
		});

		req.on('error', function(e) {
			console.log('failed to write to log server: ' + e)
		});

		req.write(payload);
		req.end();
	};

	if(!options.deferLogging) { // setup log server
		this.server = http.createServer(function(req,res) {
			req.setEncoding('utf8');
			req.on('data', function(payload) {
				var obj = JSON.parse(payload);
				_this.log("server " + obj.serverId + ": " + obj.msg);
			});

			req.on('end', function() {
				var msg = 'Message logged';
				res.writeHead(200, {
					'Content-Type': 'text/plain',
					'Content-Length': Buffer.byteLength(msg)
				});
				res.write(msg);
				res.end();
			})
		});

		this.server.listen(options.logServerPort, options.logServerHost);
		this.log("log server is listening on " + options.logServerHost + ":" + options.logServerPort);
	}

	return {
		log: this.log
	};
};

module.exports = Logger;