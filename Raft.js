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
	this.options = options;
	_this.eventBus = new EventEmitter();
	_this.storage = new Storage(_this.options.serverId);
	_this.httpServer = new HttpServer();

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
		restoreState();

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
			log("converted to " + roleNameById(_this.role) + " state: " + dumpState());

			if(_this.role === ROLES.LEADER) {
				// start sending heartbeat message
				resetHeartbeatTimeout();

				// stop election timeout
				clearElectionTimeout();
			} else {
				// stop sending heartbeat message
				clearHeartbeatTimeout();

				// Election timout must be started explictly when changing role
			}
		});
	}

	// Leader

	function resetHeartbeatTimeout() {
		clearHeartbeatTimeout();
		
		_this.heartbeatTimeout = setTimeout(function() {
			log('send heartbeat');

			// send initial empty AppendEntries RPCs (heartbeat) to each server;
			// repeat during idle periods to prevent election timeouts
			resetHeartbeatTimeout();

			for(var id in GLOBAL.servers) {
				var currentId = parseInt(id);
				// Skip self
				if(currentId === _this.serverId) {
					continue;
				}

				log('FollowerAppendEntries -> server ' + currentId);
				_this.httpServer.invokeRPC(currentId, 'FollowerAppendEntries', {
					term: _this.state.currentTerm,
					leaderId: _this.serverId,
					prevLogIndex: null,
					prevLogTerm: null,
					entries: [],
					leaderCommit: _this.commitIndex
				}, function(_currentId) {
					return function(response) { // success
						refreshTerm(response.term);

						if(_this.role != ROLES.LEADER) { // Reject responses if no longer a leader
							log('rejected FollowerAppendEntries response: not a leader');
							return;
						}

						log('FollowerAppendEntries response: ' + JSON.stringify(response));

						if(response.success === true) {
							//  update nextIndex and matchIndex for follower
						} else if(response.success === false) {
							// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
						} else {
							log('remote server responded with invalid success value: ' + JSON.stringify(response.success));
						}
					};
				}(currentId), function(e) { // error
					log('failed to invoke FollowerAppendEntries: ' + JSON.stringify(e));
				});
			}
		}, _this.heartbeatTimeoutInterval);
	}

	function clearHeartbeatTimeout() {
		if(_this.heartbeatTimeout != null) {
			clearTimeout(_this.heartbeatTimeout);
			_this.heartbeatTimeout = null;
		}
	}

	// EO Leader

	// Election phase
	function startElection() {
		setRole(ROLES.CANDIDATE); 	// convert to candidate
		_this.state.currentTerm++;	// increment current Term
		_this.state.votedFor = _this.serverId; // vote for self
		resetElectionTimeout(); 	// reset election timeout

		if(!GLOBAL.servers) {
			throw 'GLOBAL.servers is undefined';
		}

		log('start election');

		var votes = 1; // voted for self

		// Send RequestVote RPCs to all other servers
		for(var id in GLOBAL.servers) {
			var currentId = parseInt(id);
			// Skip self
			if(currentId === _this.serverId) {
				continue;
			}

			var lastLogIndex = _this.state.log.length - 1;
			log('RequestVote -> server ' + currentId);

			_this.httpServer.invokeRPC(currentId, 'RequestVote', {
				term: _this.state.currentTerm,
				candidateId: _this.serverId,
				lastLogIndex: lastLogIndex,
				lastLogTerm: lastLogIndex >= 0 ? _this.state.log[lastLogIndex].term : null
			}, function(_currentId) {
				return function(response) { // success
					refreshTerm(response.term);

					if(_this.role != ROLES.CANDIDATE) { // Reject response if no longer a candidate
						log('rejected RequestVote response: not a candidate');
						return;
					}

					if(response.voteGranted === true) {
						votes++;
						log('got voted by server ' + _currentId + ', total positive votes: ' + votes);

						// If votes received from majority of servers: become leader
						if(votes > GLOBAL.config.numServers/2 ) {
							log('elected as leader');
							setRole(ROLES.LEADER);
						}
					} else if (response.voteGranted === false) {
						log('vote rejected by server ' + _currentId);
					} else {
						log('remote server responded with invalid voteGranted value: ' + JSON.stringify(response.voteGranted));
					}
				};
			}(currentId), function(e) { // error
				log('failed to invoke RequestVote: ' + JSON.stringify(e));
			});
		}
	}

	function resetElectionTimeout() {
		clearElectionTimeout();
		//var next = new Date();
		//next.setMilliseconds(next.getMilliseconds() +  _this.options.electionTimeoutInterval);
		//log('resetElectionTimeout: next election in ' + next.toISOString());
		_this.electionTimeout = setTimeout(startElection, _this.options.electionTimeoutInterval);
	}

	function clearElectionTimeout() {
		if(_this.electionTimeout != null) {
			clearTimeout(_this.electionTimeout);
			_this.electionTimeout = null;
		}
	}
	// EO Election phase

	function applyLogEntry(index, sucess, error) {
		log('Applying log entry ' + index);
		success();
	}

	function log(msg) {
		if(GLOBAL.logger) {
			GLOBAL.logger.log('(Raft) ' + msg);
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

	function setTerm(newTerm) {
		_this.state.currentTerm = newTerm;
		_this.state.votedFor = null;
	}

	function refreshTerm(recievedTerm) {
		// All servers: if RPC request or response contains term T > currentTerm: 
		// set currentTerm = T, convert to follower 
		if(recievedTerm > _this.state.currentTerm) {
			setTerm(recievedTerm);
			setRole(ROLES.FOLLOWER);
			resetElectionTimeout();
		}
	}

	// Utilities

	function restoreState() {
		var _state = _this.storage.restore();
		_this.state = _state || _this.state;

		log('state restored: ' + dumpState());
	}

	function persistState() {
		_this.storage.persist(_this.state);
	}

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

	initialize(_this.options);

	//////////////////////////////////////////////////
	// Public interface (RPCs)
	//////////////////////////////////////////////////

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
	this.requestVote = function(params, success, error) {
		log('<- RequestVote');

		persistState();

		var response = {
			term: _this.state.currentTerm
		};

		refreshTerm(params.term);

		// Vote
		if(params.term < _this.state.currentTerm) { // Reply false if term < currentTerm
			response.voteGranted = false;
		} else if (_this.state.votedFor === null || _this.state.votedFor === params.candidateId) {
			// If votedFor is null or candidateId, and candidate's log is at 
			// least as up-to-date as receiver’s log, grant vote
			response.voteGranted = true;
		} else {
			response.voteGranted = false;
		}

		success(response);
	};

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
	this.followerAppendEntries = function(params, success, error) {
		log('<- FollowerAppendEntries');

		persistState();

		var response = {
			term: _this.state.currentTerm
		};

		refreshTerm(params.term);
		
		if(_this.role === ROLES.FOLLOWER) {
			resetElectionTimeout(); // reset election timeout to delay election
		} else if (_this.role === ROLES.CANDIDATE) {
			setRole(ROLES.FOLLOWER);
		}

		// Reply false if term < currentTerm (reject RPC calls from stale leaders)
		if(params.term < _this.state.currentTerm) {
			log('FollowerAppendEntries: rejected call from stale leader');
			response.success = false;
			success(response);
			return;
		}

		// Update leader
		_this.currentLeader = params.leaderId;

		// Return success
		response.success = true;
		success(response);

		return;
	};

	/*
	// Invoked by client to append a new entry to the log. Follower redirect this call to leader.
	// params: {
	//	 entry
	// }
	*/
	this.appendEntry = function(params, sucess, error) {
		log('<- AppendEntry');

		persistState();

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
	};

	// Register HTTP handlers
	this.httpServer.regsiterMethod('RequestVote', this.requestVote);
	this.httpServer.regsiterMethod('FollowerAppendEntries', this.followerAppendEntries);
	this.httpServer.regsiterMethod('AppendEntry', this.appendEntry);

	return {
		start: function() {
			_this.httpServer.create(_this.host, _this.port);
		}
	};
};

module.exports = Raft;