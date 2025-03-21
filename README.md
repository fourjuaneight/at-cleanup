# AT Cleanup

Script to remove content from Bluesky (AT Network).

Directly taken from [Jaz's](https://bsky.app/profile/jaz.bsky.social) [bsky-experiments](https://github.com/ericvolp12/bsky-experiments).

## Install

There are 3 ways to run the script:

### Gorun
```sh
make run
# script should run from root of repo
./atCleanup.go
```

### Local Binary
```sh
make build
# binary should be accessible from root of repo
./atCleanup
```

### GOPATH Binary
```sh
make install
# binary should be accessible from anywhere
atCleanup
```