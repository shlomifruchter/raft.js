var http = require('http');

var Logger = function(options) {

	var _this = this;
	this.options = options;

	this.log = function(msg) {
		var timestamp = (new Date()).toISOString();
		msg = timestamp + " --- " + msg;
		if(options.deferLogging) {
			_this._deferLog(msg);
		} else {
			console.log(msg);
		}
	};

	this._deferLog = function(msg) {
		var messageJson = {
			msg: msg,
			serverId: _this.options.serverId
		};

		var payload = JSON.stringify(messageJson);
		var options = {
			host: _this.options.logServerHost,
			port: _this.options.logServerPort,
			path: '/',
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Content-Length': Buffer.byteLength(payload)
			}
		};

		var req = http.request(options);

		req.on('error', function(e) {
			console.log('failed to write to log server: ' + JSON.stringify(e) + ', payload: ' + payload + ', options' + JSON.stringify(options));
		});

		req.write(payload);
		req.end();
	};

	if(options.listenForRemoteLogs) { // setup log server
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