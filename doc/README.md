This package provides a layer for using FoundationDB as a data store for a
Swift application.

# Building

## General

Before you can build this library, you must install the FoundationDB client
libraries and the FoundationDB server from our
[website](https://www.foundationdb.org/download/).

You will also need to set up a local cluster. There should be an fdbserver
process that was started automatically when you installed the server. If it's
not running, you can run:

    fdbserver -p auto:4689 -d /usr/local/foundationdb/data/4689 -L /usr/local/foundationdb/logs &

Once it's running, you will need to configure the initial database:

    fdbcli --exec "configure new single ssd"
    fdbcli --exec status

This currently builds with Swift 4.0.

## Mac Command Line

To build on the Mac, you must manually install the [pkg-config](https://github.com/FoundationDB/fdb-swift-bindings/blob/master/Sources/CFoundationDB/include/CFoundationDB_mac.pc)
file in `/usr/local/lib/pkgconfig/CFoundationDB.pc`.

Then you can run `swift build` to build the library, and `swift test` to run the
test suite.

## Xcode

You can generate an Xcode project for this package by running
`swift package generate-xcodeproj` from the
command-line. You should then be able to build the library and run the test
suite in Xcode.

## Linux

This repository provides a docker image for building the library in Linux. You
can build and run the tests by running:

        docker run --rm -v $PWD:/var/code/fdb-swift fdb-swift-build
        
You can build that Dockerfile by running:

        docker build -t fdb-swift-build Resources/docker

When switching from building for Mac and building for Linux or vice-versa, run
`swift build --clean`.

## Updating Versions

This package must be kept synchronized with new versions of FoundationDB. To
update the version for this package:

1.  Check out the master branch of the
    [C Wrapper](https://github.com/apple/fdbc-swift) project,
    and update the versions in the following files:

    -   CFoundationDB.h
    -   CFoundationDB_linux.pc
    -   CFoundationDB_mac.pc

2.  Commit those changes and push them to the remote repo
3.  Tag the master branch with the new version (e.g. `5.0.5`) and push that tag
    to the remote repo.
4.  Update the `resources/docker/Dockerfile` file with the new FDB version
5.  Build the new docker image:

        docker build resources/docker -t fdb-swift-build

6.  Update the dependency on fdbc-swift in `Package.swift` in this repo to
    specify the new version.
7.  Commit those change and push them to the remote repo
8.  Tag the master branch with the new version (e.g. `5.0.5`) and push that tag
    to the remote repo.
