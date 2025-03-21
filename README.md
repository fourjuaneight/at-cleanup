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

## Usage


```sh
atCleanup --did=<DID> --password=<APP_PASS> --types=post,repost,like --days=30 --rate-limit=4 --burst-limit=1
```

### Flags
* `--did`: This is the Decentralized Identifier of the user. ([Documentation](https://docs.bsky.app/docs/advanced-guides/resolving-identities))
* `--password`: The application password associated with the user's account. ([Create an app password here](https://bsky.app/settings/app-passwords))
* `--types`: A comma-separated list of record types you want to clean up.
  * `post`
  * `repost`
  * `like`
* `--days`: The number of days beyond which records should be considered for deletion.
* `--rate-limit`: The rate at which requests to delete records are sent. The default value here is 4, which corresponds to 4 requests per second.
* `--burst-limit`: The maximum burst of requests sent at once.
