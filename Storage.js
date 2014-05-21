var fs = require('fs');

// Simple synchronous file-based persistence storage module
var Storage = function(serverId) {
	var path = "data/";
	var filename = "server-data-" + serverId;
	var _this = this;

	this.persist = function(state){
		fs.writeFileSync(path + filename, JSON.stringify(state));
	};

	this.restore = function() {
		try {
			var data = fs.readFileSync(path + filename, {
				encoding: 'utf-8'
			});
			return JSON.parse(data);
		} catch (e) {
			return null;
		}
	};
};

module.exports = Storage;