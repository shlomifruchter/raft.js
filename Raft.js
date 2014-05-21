var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Storage = require('./Storage');
var HttpServer = require('./HttpServer');

var ROLES = {
	FOLLOWER: 0,
	CANDIDATE: 1,
	LEADER: 2
};

var Raft = function(options) {
	//////////////////////////////////////////////////
	// Private attributes
	//////////////////////////////////////////////////

	var _this = this;
	this.eventBus = new EventEmitter();
	this.storage = new Storage(options.serverId);
	this.httpServer = new HttpServer(this); // TODO: remove coupling between HttpServer and Raft by implementing an event based communication

	// Persistent state
	this.state = {
		currentTerm: 0,
		votedFor: null,
		log: [] // log index is zero-based
	};

	// Volatile state on all servers
	this.commitIndex = 0;
	this.lastApplied = 0;
	this.currentLeader = null;

	this.role = ROLES.FOLLOWER; // all nodes starts as followers
	this.electionTimeout = null;
	this.heartbeatTimeout = null;

	// Volatile state on leaders
	this.nextIndex = {};
	this.matchIndex = {};

	//////////////////////////////////////////////////
	// Private methods
	//////////////////////////////////////////////////

	function initialize(options) {
		var _state = _this.storage.restore();
		_this.state = _state || _this.state;
		_this.serverId = options.serverId;
		_this.electionTimeoutInterval = options.electionTimeoutInterval || 500;
		_this.heartbeatTimeoutInterval = options.heartbeatTimeoutInterval || 250;
		_this.host = options.host || 'localhost';
		_this.port = options.port;

		setupEvents();
		resetElectionTimeout(); // start election timeout

		log('Raft initialized');
	};

	function setupEvents() {
		_this.eventBus.on('commitIndex:change', function() {
			if(_this.commitIndex > _this.lastApplied) {
				try {
					applyLogEntry(_this.lastApplied+1);
					_this.lastApplied++;
				} catch(e) {
					log('failed to apply log entry ' + (_this.lastApplied+1));
				}
			}
		});

		_this.eventBus.on('role:change', function(oldRole, newRole) {
			if(_this.role == ROLES.LEADER) {
				// start sending heartbeat message
				resetHeartbeatTimeout();
			} else {
				// stop sending heartbeat message
				clearTimeout(_this.heartbeatTimeout);
			}

			log("converted to " + roleNameById(_this.role));
		});
	}

	// Leader

	function resetHeartbeatTimeout() {
		if(_this.heartbeatTimeout) {
			clearTimeout(heartbeatTimeout);
		}
		_this.heartbeatTimeout = setTimeout(function() {
			// send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
			resetHeartbeatTimeout();

			for(var id in GLOBAL.servers) {
				httpServer.invokeRPC(id, 'followerAppendEntries', {
					term: _this.state.currentTerm,
					leaderId: _this.serverId,
					prevLogIndex: null,
					prevLogTerm: null,
					entries: [],
					leaderCommit: _this.commitIndex
				}, function(response) { // success

				}, function(response) { // error

				});
			}
		}, _this.heartbeatTimeoutInterval);
	}

	// EO Leader

	// Election phase
	function startElection() {

		setRole(ROLES.CANDIDATE); // convert to candidate

		_this.state.currentTerm++;
		_this.votedFor = _this.serverId;
		resetElectionTimeout();

		if(GLOBAL.servers) {
			log('start election');
			var votes = 0;

			// Send RequestVote RPCs to all other servers
			for(var id in GLOBAL.servers) {
				var lastLogIndex = _this.state.log.length - 1;
				log('RequestVote -> server ' + id);
				_this.httpServer.invokeRPC(id, 'RequestVote', {
					term: _this.state.currentTerm,
					candidateId: _this.serverId,
					lastLogIndex: lastLogIndex,
					lastLogTerm: lastLogIndex >= 0 ? _this.state.log[lastLogIndex].term : null
				}, function(response) { // success
					if(_this.role != ROLES.CANDIDATE) { // Reject responses if no longer a candidate
						return;
					}

					if(response.voteGranted) {
						votes++;

						// If votes received from majority of servers: become leader
						if(votes > (GLOBAL.servers.length/2 + 1) ) {
							setRole(ROLES.LEADER);
						}
					}
				}, function(response) { // error
					log('Remote server responded with error in response to RequestVote: ' + JSON.stringify(response));
				});
			}
		} else {
			throw 'GLOBAL.servers is undefined';
		}
	}

	function resetElectionTimeout() {
		if(_this.electionTimeout) {
			clearTimeout(_this.electionTimeout);
		}
		_this.electionTimeout = setTimeout(startElection, options.electionTimeoutInterval);
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

	// Mutators

	function setCommitIndex(newCommitIndex) {
		commitIndex = newCommitIndex;
		eventBus.emit('commitIndex:change');
	}

	function setRole(newRole) {
		if(_this.role == newRole) {
			return;
		}

		_this.role = newRole;
		_this.eventBus.emit('role:change');
	}

	// Utilities

	function roleNameById(roleId) {
		switch(roleId) {
			case 0:
				return "follower";
			case 1:
				return "candidate";
			case 2:
				return "leader";
		}

		return "unknown role";
	}

	function dumpState() {
		var stateSnapshot = {
			currentTerm: _this.state.currentTerm,
			votedFor: _this.state.votedFor,
			log: _this.state.log,
			commitIndex: _this.commitIndex,
			lastApplied: _this.lastApplied,
			currentLeader: _this.currentLeader,
			role: roleNameById(_this.role),
			nextIndex: _this.nextIndex,
			matchIndex: _this.matchIndex
		};

		return JSON.stringify(stateSnapshot);
	}

	// EO Utilities

	// Initialization

	initialize(options);

	//////////////////////////////////////////////////
	// Public interface
	//////////////////////////////////////////////////
	return {
		start: function() {
			_this.httpServer.create(_this.host, _this.port);
		},
		/*
		// RequestVote RPC
		// 
		// params: {
		//	term,
		//	candidateId,
		//	lastLogIndex,
		//  lastLogTerm
		// }
		*/
		requestVote: function(params, success, error) {
			var response = {};
			if(params.term < _this.state.currentTerm) { // Reply false if term < currentTerm
				response.term = _this.state.currentTerm;
				response.voteGranted = false;
			} else if (_this.state.votedFor == null || _this.state.votedFor == params.candidateId) {
				// If votedFor is null or candidateId, and candidate's log is at 
				// least as up-to-date as receiver’s log, grant vote
				resetElectionTimeout();
			}

			success(response);
		},
		/* 
		// AppendEntries RPC
		//
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
			var response = {
				term: _this.state.currentTerm
			};

			// Reply false if term < currentTerm (reject RPC calls from stale leaders)
			if(params.term < _this.state.currentTerm) {
				response.success = false;
				success(response);
				return;
			}

			// Update leader, reset election timeout
			_this.currentLeader = leaderId;
			resetElectionTimeout();

			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			if(params.term > _this.state.currentTerm) {
				_this.state.currentTerm = params.term;
				setRole(ROLES.FOLLOWER);
			}
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
			if(_this.role == ROLES.LEADER) {
				_this.state.log.push({
					term: _this.state.currentTerm,
					entry: params.entry
				});
				applyLogEntry(_this.state.log.length - 1, success, error);

				// send 
				resetHeartbeatTimeout();
			} else { // followers and candidates redirect calls to leader
				if(_this.currentLeader) {
					success({
						redirect: _this.currentLeader
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

module.exports = Raft;