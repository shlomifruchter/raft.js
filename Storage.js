var fs = require('fs');

// Simple synchronous file-based persistence storage module
var Storage = function(serverId) {
	var dataDirectoryName = "data";
	var filename = "server-data-" + serverId;
	var _this = this;

	// synchronously persist the state to disk
	this.persist = function(state){
		var path = __dirname + '/' + dataDirectoryName;
		if(!fs.existsSync(path)) {
			try {
				fs.mkdirSync(path);
			} catch(e) {
				log('failed to create data directory: ' + e);
			}
		}
		fs.writeFileSync(path + '/' + filename, JSON.stringify(state));
	};

	this.restore = function() {
		try {
			var path = __dirname + '/' + dataDirectoryName;
			var data = fs.readFileSync(path + '/' + filename, {
				encoding: 'utf-8'
			});
			return JSON.parse(data);
		} catch (e) {
			log('failed to restore state: ' + e);
			return null;
		}
	};

	function log(msg) {
		if(GLOBAL.logger) {
			GLOBAL.logger.log('(Raft) ' + msg);
		}
	}
};

module.exports = Storage;