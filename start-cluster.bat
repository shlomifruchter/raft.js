REM start node server logger 0
rmdir /s /q data
rmdir /s /q logs
start node server raft 1
start node server raft 2
start node server raft 3
start node server raft 4
start node server raft 5