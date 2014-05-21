var events = require('events');
var Storage = require('./Storage');

var ROLES = {
		FOLLOWER: 0,
		CANDIDATE: 1,
		LEADER: 2
	};

var Raft = function(options) {
	var _this = this;
	var eventBus = new events.EventEmitter();
	var storage = new Storage(options.serverId);

	// Persistent state
	var state = {
		currentTerm: 0,
		votedFor: null,
		log: [] // log index is zero-based
	};

	var role = ROLES.FOLLOWER; // all nodes starts as followers

	// Volatile state on all server
	var commitIndex = 0;
	var lastApplied = 0;
	var currentLeader = null;

	// Volatile state on leaders
	var nextIndex = {};
	var matchIndex = {};

	// Private methods
	function initialize(options) {
		var _state = storage.restore();
		state = _state || state;
		_this.serverId = options.serverId;
		_this.electionTimeoutInterval = options.electionTimeoutInterval || 500;
		_this.host = options.host || 'localhost';
		_this.port = options.port;

		setupHttpServer();
		setupEvents();

		log('Raft initialized');
	};

	function setupEvents() {
		eventBus.on('commitIndex:change', function() {
			if(commitIndex > lastApplied) {
				try {
					applyLogEntry(lastApplied+1);
					lastApplied++;
				} catch(e) {
					log('failed to apply log entry ' + (lastApplied+1));
				}
			}
		});

		eventBus.on('role:change', function(oldRole, newRole) {
			if(role == ROLES.LEADER) {
				// start sending heartbeat message
			} else {
				// stop sending heartbeat message
			}

			if(role == ROLES.CANDIDATE) {
				// start election
			}
		});
	}

	// Election phase
	var electionTimeout = null;
	function startElection() {
		state.currentTerm++;
		votedFor = _this.serverId;
		resetElectionTimeout();

		if(GLOBAL.servers) {
			// Send RequestVote RPCs to all other servers
			for(var id in GLOBAL.servers) {
				this.emit('invokeRPC', id, 'RequestVote', {
					term: state.currentTerm,
					candidateId: _this.serverId,
					lastLogIndex: log.length - 1,
					lastLogTerm: log[log.length - 1].term
				}, function() { // success

				}, function() { // error

				});
			}
		} else {
			throw 'GLOBAL.servers is undefined';
		}
	}

	function resetElectionTimeout() {
		if(electionTimeout) {
			clearTimeout(electionTimeout);
		}
		electionTimeout = setTimeout(startElection, options.electionTimeoutInterval);
	}

	// EO Election phase

	function applyLogEntry(index, sucess, error) {
		log('Applying log entry ' + index);
		success();
	}

	function log(msg) {
		if(GLOBAL.logger) {
			GLOBAL.logger.log(msg);
		}
	}

	function setCommitIndex(newCommitIndex) {
		commitIndex = newCommitIndex;
		eventBus.emit('commitIndex:change');
	}

	function setRole(newRole) {
		if(role == newRole) {
			return;
		}

		role = newRole;
		eventBus.emit('role:change');
	}

	initialize(options);

	// Public
	return {
		/* 
		// params: {
		//	term,
		//	candidateId,
		//	lastLogIndex,
		//  lastLogTerm
		// }
		*/
		function requestVote(params, success, error) {


			var result = {};
			if(params.term < state.currentTerm) { // Reply false if term < currentTerm
				result.term = state.currentTerm;
				result.voteGranted = false;
			} else {
				// If votedFor is null or candidateId, and candidate's log is at 
				// least as up-to-date as receiver’s log, grant vote
			}

			success(result);
		},
		/* 
		// Invoked by leader to replicate log entries, also used as heartbeat
		// params: {
		//	term,
		// 	leaderId,
		//	prevLogIndex,
		// 	prevLogTerm,
		//  entries,
		//  leaderCommit
		// }
		*/
		followerAppendEntries: function(params, success, error) {
			currentLeader = leaderId;
		},
		/*
		// Invoked by client to append a new entry to the log. Follower redirect this call to leader.
		// params: {
		//	 entry
		// }
		*/
		appendEntry: function(params, sucess, error) {
			// Leader: if command received from client, append entry to local log, 
			// respond after entry applied to state machine
			if(role == ROLES.LEADER) {
				log.push({
					term: state.currentTerm
					entry: params.entry
				});
				applyLogEntry(log.length - 1, success, error);
			} else { // followers and candidates redirect calls to leader
				if(currentLeader) {
					success({
						redirect: currentLeader
					});
				} else {
					error({
						message: 'Cannot append entry: no leader is known'
					});
				}
			}
		}
	};
};

Raft.prototype.__proto__ = EventEmitter.prototype;

module.exports = Raft;