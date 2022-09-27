
Project 1
=========

The repository contains a framework and skeleton code for a peer-to-peer file sharing system using a hybrid client/server and peer-to-peer architecture. In this architecture there are two components:

- **Index Server** : a process that acts as a server, to manage an index of files and IP addresses of file sharers
- **Peer** : a process that acts as both a client to an Index Server, and as a server for sharing file data with other Peers

This is the basic architecture of early and successful file sharing systems such as *Napster* and *BitTorrent*. The Index Server is sometimes called a *tracker*, that keeps track of which Peers are sharing files, with the essential details of the files.

## Skeleton code

The skeleton code in this repository contains a starting point for the file sharing system, with the following package structure:

- `comp90015.idxsrv.Filesharer.java` : main class for the Peer component [DO NOT EDIT]
- `comp90015.idxsrv.IdxSrv.java` : main class for the Index Server component [DO NOT EDIT]
- `comp90015.idxsrv.filemgr` : package providing file blocking and hashing management [DO NOT EDIT]
- `comp90015.idxsrv.message` : package providing a JSON annotation message factory [ADD CLASSES ONLY]
- `comp90015.idxsrv.peer` : package providing the Peer implementation [EDIT `Peer.java` and ADD CLASSES]
- `comp90015.idxsrv.server` : package providing the Index Server implementation [DO NOT EDIT]
- `comp90015.idxsrv.textgui` : package providing the terminal text GUI interface [DO NOT EDIT]

The only parts of the code that students should modify are indicated above. In particular students may: *(i)* add classes into the `comp90015.idxsrv.message` package as required, *(ii)* edit `Peer.java` and complete the functionality required by the interface methods, and may add new classes to this package as required, and *(iii)* must not edit any other code or any other packages in any way.

### Changes to the Skeleton code

The Skeleton code may need to be updated throughout the project. If students follow the above rules, then changes to the Skeleton code can be pulled from this repository as they are released.

### Compiling and running the Server

    cd idxsrv
    mvn assembly:single
    java -cp target/idxsrv-0.0.1-SNAPSHOT-jar-with-dependencies.jar comp90015.idxsrv.IdxSrv

Use the `-h` option for help on options to change server configuration settings when running the server.

### Compiling and running the Peer

    cd idxsrv
    mvn assembly:single
    java -cp target/idxsrv-0.0.1-SNAPSHOT-jar-with-dependencies.jar comp90015.idxsrv.Filesharer

The Peer uses a terminal text GUI that is best run in a UNIX/Linux terminal. When the application runs the `h` key can be pressed for help on using the application. However the Peer currently does not implement the functionality required by the GUI. Implementing this functionality is the main programming task of the project.

## Server

The Server code must not be modified. The Server accepts TCP connections and uses a request-per-connection session protocol as defined in `Server.java`. The session consists of a welcome message, an authentication step, and then a single request using a request-reply protocol. All messages are formatted as JSON Objects and transmitted as strings with a new line character as a delimiter, i.e. one message per line. The client should make use of the following messages for the session handshake (welcome and authentication): 

- `WelcomeMsg` : sent by the server as the first message on any new connection
- `AuthenticateRequest` : sent by the client to request authentication
- `AuthenticateReply` : sent by the server in response to a `AuthenticateRequest`

Examine the `Server.java` code for examples of how to make use of the `MessageFactory` when sending and receiving messages, and see further in for additional description.

In some circumstances the following special message may be sent by the server instead of a reply:

- `ErrorMsg` : an error has occurred

Following the handshake, a single request can be made, using one of the following messages:

- `ShareRequest` : allows the Peer to provide the Index Server with details of a file to index
- `DropShareRequest` : allows the Peer to request the Index Server to remove the details of a file that it previously indexed
- `SearchRequest` : allows the Peer to search for files on the Index Server
- `LookupRequest` : allows the Peer to obtain a list of Peers that are sharing a given file

Each of the requests above has an accompanying reply message from the Server.

## Peer

The main programming task for Project 1 is to implement the Peer functionality, as required by the `IPeer.java` interface definition. Most of the functionality is interacting with the server as described above, however some functionality requires transferring file data between peers. This will require some additional messages to be added to the `message` package. The Peer must also call relevant methods defined in the `ISharerGUI.java` interface definition to update the GUI with the relevant results.

### Sharing file data in blocks

All Peers must follow the same protocol to share file data, in the form of blocks. The session protocol between two peers is simply an unbounded number of block request-reply exchanges followed by a goodbye message; this is an example of multiple-request-per-connection.
The following two messages must be created with class names *exactly* as written below and added to `message` package to do this:

- `BlockRequest` : allows a peer to request a block from another peer
- `BlockReply` : allows a peer to reply to a block request
- `Goodbye` : to indicate that no further block request messages will follow

The fields of the `BlockRequest` should contain exactly the following:

- `String filename` : the name of the file containing the requested block
- `String fileMd5` : the MD5 hash of the file (provided by the `FileDescr` class)
- `Integer blockIdx` : the index of the block requested

The fields of the `BlockReply` should contain exactly the following:

- `String filename` : the name of the file for which the block data is for
- `String fileMd5` : the hash of the file for which the block data is for (provided by the `FileDescr` class)
- `Integer blockIdx` : the index of the provided block
- `String bytes` : a Base 64 Encoded string representing the byte array of the block

The `Goodbye` message does not require any fields.

The Peer may implement any strategy to eventually download all blocks of a file from whatever other peers are sharing that file. Concurrent file sharing, i.e. downloading/uploading file blocks concurrently, should be possible.

The Peer may make use of `ErrorMsg` in lieu of a reply to indicate that an error has occurred, i.e. that the requested blocks could not be provided.

## `message` package

Messages in this project use the JSON format, however the programmer must use JSON annotations, as defined in the `message` package, to annotate classes and fields that are *JSON Serializable*. A JSON Serializable class can be serialized to a JSON string, and conversely a JSON string can be de-serialized into a class using the `MessageFactory`. Fields may be primitive JSON Elements or otherwise other JSON Serializable classes. Other fields are ignored.

Internally, the `MessageFactory` uses Java Reflection and the intermediate JSON object will contain a reserved field name, `_class`, that is the name of the JSON serializable class. This is used to create an appropriate Java object when parsing the JSON string at the receiver.

## `filemgr` package

The `filemgr` package provides a standard method for dividing a file into constant size blocks, creating hashes for those blocks, and managing a random access file descriptor for reading and writing blocks to a file, with functionality to check which blocks are required and which are available. All access to files should be done through the use of the `filemgr` package.

## Terminal text GUI

The Peer must make use of the `ISharerGUI` interface to update the GUI with all relevant information, including logging information to the terminal. The Peer cannot use standard system I/O. 

## Tasks for Project 1

In this project, the following tasks must be completed:

- Implement the Peer functionality in `Peer.java` required by the GUI, as described in the `IPeer.java` interface definition.
- Prepare a written submission. The details of the written submission will be provided separately.

