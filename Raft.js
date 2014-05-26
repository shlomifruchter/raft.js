var utils = require('./utils');
var config = require('./config');
var EventEmitter = require('events').EventEmitter;
var Storage = require('./Storage');
var HttpServer = require('./HttpServer');

var ROLES = {
	FOLLOWER: 0,
	CANDIDATE: 1,
	LEADER: 2
};

var ERROR_CODES = {
	E_SUCCESS: 0,
	E_INCONSISTENT_PREV_ENTRY: 1,
	E_STALE_LEADER: 2
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
	this.commitIndex = -1; // index is 0-based, unlike in Raft paper
	this.lastApplied = -1; // index is 0-based, unlike in Raft paper
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
	}

	function setupEvents() {
		_this.eventBus.on('commitIndex:change', function() {
			var currentLogIndex = _this.lastApplied + 1;

			while(currentLogIndex <= _this.commitIndex) {
				try {
					applyLogEntry(currentLogIndex);
				} catch(e) {
					log('failed to apply log entry ' + currentLogIndex);
				}

				currentLogIndex++;
			}
		});

		_this.eventBus.on('role:change', function(oldRole, newRole) {
			log("converted to " + roleNameById(_this.role) + " state: " + dumpState());

			if(_this.role == ROLES.LEADER) {
				// start sending heartbeat message
				resetHeartbeatTimeout();

				// stop election timeout
				clearElectionTimeout();

				// initialize nextIndex[] and matchIndex[]
				var lastLogIndex = _this.state.log.length - 1;
				for(var id in GLOBAL.servers) {
					var currentId = parseInt(id);
					// Skip self
					if(currentId === _this.serverId) {
						continue;
					}

					_this.nextIndex[currentId] = lastLogIndex + 1;
					_this.matchIndex[currentId] = -1; // index is 0-based, unlike in Raft paper
				}
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
				}, function(response) { // success
					if(response === undefined) {
						log('heartbeat FollowerAppendEntries: invalid response');
					}

					refreshTerm(response.term);

					if(_this.role !== ROLES.LEADER) { // Reject responses if no longer a leader
						log('rejected FollowerAppendEntries response: not a leader');
						return;
					}

					log('FollowerAppendEntries response: ' + JSON.stringify(response));
				}, function(e) { // error
					log('failed to invoke FollowerAppendEntries: ' + JSON.stringify(e));
				});
			}
		}, _this.heartbeatTimeoutInterval);
	}

	function clearHeartbeatTimeout() {
		if(_this.heartbeatTimeout !== null) {
			clearTimeout(_this.heartbeatTimeout);
			_this.heartbeatTimeout = null;
		}
	}

	function appendEntriesToRemoteServer(remoteServerId, success, error) {
		log('appendEntriesToRemoteServer: remoteServerId=' + remoteServerId);
		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		var lastLogIndex = _this.state.log.length - 1;
		if(lastLogIndex >= _this.nextIndex[remoteServerId]) {
			var prevLogIndex = _this.nextIndex[remoteServerId] - 1; // should be -1 for first entry
			var prevLogTerm = prevLogIndex >= 0 ? _this.state.log[prevLogIndex].term : null;
			var entries = _this.state.log.slice(prevLogIndex+1);

			log('FollowerAppendEntries -> server ' + remoteServerId + ' , sending ' + entries.length + ' entries');

			_this.httpServer.invokeRPC(remoteServerId, 'FollowerAppendEntries', {
				term: _this.state.currentTerm,
				leaderId: _this.serverId,
				prevLogIndex: prevLogIndex,
				prevLogTerm: prevLogTerm,
				entries: entries,
				leaderCommit: _this.commitIndex
			}, function(_currentId) {
				return function(response) { // success
					if(response === undefined) {
						log('appendEntriesToRemoteServer:FollowerAppendEntries: invalid response');
					}

					refreshTerm(response.term);

					if(_this.role != ROLES.LEADER) { // Reject responses if no longer a leader
						log('rejected FollowerAppendEntries response: not a leader');
						error({
							message: 'rejected FollowerAppendEntries response: not a leader'
						});
						return;
					}

					log('FollowerAppendEntries response: ' + JSON.stringify(response));

					if(response.success === true) {
						//  update nextIndex and matchIndex for follower
						_this.nextIndex[_currentId] = lastLogIndex + 1; // index of the next log entry to send to that server
						_this.matchIndex[_currentId] = lastLogIndex; // index of highest log entry known to be replicated on server
						success(response);
						return;
					} else if(response.success === false) {
						// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
						if(response.errorCode === ERROR_CODES.E_INCONSISTENT_PREV_ENTRY) {
							_this.nextIndex[_currentId]--;
							appendEntriesToRemoteServer(remoteServerId, success, error); // retry
							return;
						} else { // handle other errors
							error({
								message: 'replication failed with response: ' + JSON.stringify(response)
							});
							return;
						}
					} else {
						log('remote server responded with invalid success value: ' + JSON.stringify(response.success));
						error({
							message: 'remote server responded with invalid success value'
						});
						return;
					}
				};
			}(remoteServerId), function(e) { // error
				log('failed to invoke FollowerAppendEntries: ' + JSON.stringify(e));
			});
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

			var requestVoteSuccessHandler = function(_currentId, response) { // success
				refreshTerm(response.term);

				if(_this.role != ROLES.CANDIDATE) { // Reject response if no longer a candidate
					log('rejected RequestVote response: not a candidate');
					return;
				}

				if(response.voteGranted === true) {
					votes++;
					log('got voted by server ' + _currentId + ', total positive votes: ' + votes);

					// If votes received from majority of servers: become leader
					var majority = Math.floor(config.numServers/2) + 1;
					if(votes >= majority) {
						log('elected as leader, got ' + votes + ', min for winning is ' + majority);
						setRole(ROLES.LEADER);
					}
				} else if (response.voteGranted === false) {
					log('vote rejected by server ' + _currentId);
				} else {
					log('remote server responded with invalid voteGranted value: ' + JSON.stringify(response.voteGranted));
				}
			};

			_this.httpServer.invokeRPC(currentId, 'RequestVote', {
				term: _this.state.currentTerm,
				candidateId: _this.serverId,
				lastLogIndex: lastLogIndex,
				lastLogTerm: lastLogIndex >= 0 ? _this.state.log[lastLogIndex].term : null
			}, requestVoteSuccessHandler.bind(_this, currentId), function(e) { // error
				log('failed to invoke RequestVote: ' + JSON.stringify(e));
			});
		}
	}

	function resetElectionTimeout() {
		clearElectionTimeout();
		_this.electionTimeout = setTimeout(startElection, _this.options.electionTimeoutInterval);
	}

	function clearElectionTimeout() {
		if(_this.electionTimeout !== null) {
			clearTimeout(_this.electionTimeout);
			_this.electionTimeout = null;
		}
	}
	// EO Election phase

	function applyLogEntry(index, success, error) {
		log('Applying log entry ' + index);
		_this.lastApplied = index;
		if(success instanceof Function) {
			success({
				status: "commited"
			});
		}
	}

	function log(msg) {
		if(GLOBAL.logger) {
			GLOBAL.logger.log('(Raft) ' + msg);
		}
	}

	// Mutators

	function setCommitIndex(newCommitIndex) {
		_this.commitIndex = newCommitIndex;
		_this.eventBus.emit('commitIndex:change');
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

			// Compare last entries in local and candidate's logs:
			// ---------------------------------------------------

			var localLastLogIndex = _this.state.log.length - 1;

			if(localLastLogIndex >= 0 ) {
				// If the logs have last entries with different terms, then the log with the later term is more up-to-date
				if(params.lastLogTerm < _this.state.log[localLastLogIndex].term) {
					response.voteGranted = false; // local log is more up-to-date since it last log's term is later
				} 
				// If the logs end with the same term, then whichever log is longer is more up-to-date.
				else if (params.lastLogTerm === _this.state.log[localLastLogIndex].term && 
						 localLastLogIndex > params.lastLogIndex) {
					response.voteGranted = false; // local log is more up-to-date since it's longer
				} else {
					response.voteGranted = true; // remote log is at least as up-to-as as local log
				}
			} else { // local log is empty, grant vote only if remote log is empty as well
				if(params.lastLogTerm === null ) {
					response.voteGranted = true;
				} else {
					response.voteGranted = false;
				}
			}
		} else {
			response.voteGranted = false;
		}

		// Remember vote to prevent voting for another leader in this term
		if(response.voteGranted) {
			_this.state.votedFor = params.candidateId;
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

		/////////////////////////////////
		// Election and heartbeat logic
		/////////////////////////////////

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
			response.errorCode = ERROR_CODES.E_STALE_LEADER;
			success(response);
			return;
		}

		// Update leader
		_this.currentLeader = params.leaderId;

		// Non-null preLogIndex means a non-hearbeat message
		if(params.prevLogIndex !== null) {
			/////////////////////////////////
			// Log replication logic
			/////////////////////////////////

			// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
			if(params.prevLogIndex >= 0) { // make sure the prev index is not -1
				var localPrevLogEntry = _this.state.log[params.prevLogIndex];
				if(localPrevLogEntry === undefined || localPrevLogEntry.term != params.prevLogTerm) {
					log('FollowerAppendEntries: rejected call due to unmatching previous log term: localPrevLogEntry=' + JSON.stringify(localPrevLogEntry) + ' prevLogTerm=' + params.prevLogTerm);
					response.success = false;
					response.errorCode = ERROR_CODES.E_INCONSISTENT_PREV_ENTRY;
					success(response);
					return;
				}
			}
			

			var currentLogIndex = params.prevLogIndex;

			// Iterate through new log entries
			for(var i in params.entries) {
				currentLogIndex++;

				var localExistingLogEntry = _this.state.log[currentLogIndex];

				// Append any new entries not already in the log
				if(localExistingLogEntry === undefined) {
					_this.state.log[currentLogIndex] = params.entries[i];
					log('log entry ' + currentLogIndex + ' created: ' + params.entries[i].entry);
				} else { 
					// If an existing entry conflicts with a new one (same index but different terms),
					// delete the existing entry and all that follow it
					if(localExistingLogEntry.term !== params.term) {
						_this.state.log.splice(currentLogIndex); // remove all log entries starting from first new entry index
						log('log entries from index ' + currentLogIndex + ' deleted');
						_this.state.log[currentLogIndex] = params.entries[i]; // Append any new entries not already in the log
						log('log entry ' + currentLogIndex + ' created: ' + params.entries[i].entry);
					} else {
						// terms are matching - entries should match as well
						utils.assert(_this.state.log[currentLogIndex].entry === params.entries[i].entry, 'entry terms are matching - entry data should match as well');
					}
				}
			}
		}

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		// This should be handled for heartbeat messages as well
		if(params.leaderCommit > _this.commitIndex) {
			setCommitIndex(Math.min(params.leaderCommit, _this.state.log.length-1)); // currentLogIndex should be the index of the last new entry
		}

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
	this.appendEntry = function(params, success, error) {
		log('<- AppendEntry');

		persistState();

		// Leader: if command received from client, append entry to local log, 
		// respond after entry applied to state machine
		if(_this.role === ROLES.LEADER) {
			_this.state.log.push({
				term: _this.state.currentTerm,
				entry: params.entry
			});
			persistState(); // TODO: Make sure it is correct to persist state here!
			applyLogEntry(_this.state.log.length - 1, success, error);

			// delay next heartbeat, since we just sent AppendEntries RPC
			resetHeartbeatTimeout();

			try {
				// replicate log to all servers
				utils.forEachRemoteServer(GLOBAL.servers, _this.serverId, function(currentId) {
					appendEntriesToRemoteServer(currentId, function(response) { // success
						// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
						// and log[N].term == currentTerm: set commitIndex = N

						log('Check if commit index should be changed');

						// Check if commit index should be changed
						var N = _this.state.log.length - 1;
						while(N > _this.commitIndex) {
							if(_this.state.log[N].term === _this.state.currentTerm) {

								// count {i : matchIndex[i] ≥ N}
								var count = 0;
								utils.forEachRemoteServer(GLOBAL.servers, _this.serverId, function(sid) {
									if(_this.matchIndex[sid] >= N) {
										count++;
									}
								});

								var majority = Math.floor(config.numServers/2) + 1;
								if(count >= majority) {
									log('commit index was set to ' + N);
									setCommitIndex(N);
								}
								
								break;
							}

							N--;
						}
					}, function(response) { // error
						log('appendEntriesToRemoteServer failed, response: ' + JSON.stringify(response));
					});
				});
			} catch(e) {
				log('exception thrown while appending entries to remote servers: ' + e);
			}
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