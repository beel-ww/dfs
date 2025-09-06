A distributed file system application based on a UBC CS416 assignment.

[UBC page](https://www.cs.ubc.ca/~bestchai/teaching/cs416_2017w2/assign2/index.html)

Original Stub Files
[dfslib.go](https://www.cs.ubc.ca/~bestchai/teaching/cs416_2017w2/assign2/dfslib.go)
[app.go](https://www.cs.ubc.ca/~bestchai/teaching/cs416_2017w2/assign2/app.go)

All code other than what is in the original stubs is written by me.
This is my first project using golang. I had no prior experience with RPCs or implementing distributed systems.

# Usage
Please ensure that localPath exists before running the application. By default the included application.go passes "/tmp/dfs-dev/" as the local path. If the directory doesn't exist, nothing else will work. The server should also be running before the application is started as well.

The addresses used in app.go and server/server.go should be changed if appropriate. The addresses for both are currently localhost.

# Current To-dos/Wishlist

- improve error handling
- lots of refactoring
- rewrite using GRPC
- testing

# Docker
Dockerfile is included for building an image for containerization. As noted above, please make sure to change the IP addresses used where appropriate.