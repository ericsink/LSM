
# Overview

NOTE:  Nothing here is ready for production use.

This repo contains a bunch of database stuff in Rust:

## lsm

A key-value store built on a Log Structured Merge Tree.

This library was originally written in C#, and that version of the code
exists in the history of this repo somewhere.  

Then it was
ported to F#, and that version is in the fsharp directory.

Then it was ported to Rust.

## elmo

A NoSQL/document database aimed primarily at mobile use.
This is a Rust port of https://github.com/zumero/Elmo

## bson

The BSON code from Elmo, separated out as a library.

## server

The server and wire protocol parts of Elmo, separated out as
a library.  This code exists primarily for the purpose of
allowing the MongoDB test suite to be run against the
Elmo database library.

## storage/sqlite3

An implementation of Elmo's storage API which uses SQLite as
a storage layer.

## misc

A library of common utility stuff that is used by the 
projects above.

# Status

For LSM, the port from F# to Rust is mostly complete.

For all the ELmo stuff, the Rust port is just getting started.

# License

Apache v2


