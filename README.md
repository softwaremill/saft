# Saft: a Scala Raft implementation

An implementation of the [Raft](https://raft.github.io) consensus algorithm, using the [Scala](https://scala-lang.org) language and a functional effect system ([zio](https://zio.dev)). Currently, the goal of this project is educational, not production usage.

If you're new to Scala, you might want to setup your environment first, installing Java and an IDE. [This page](https://scala.page) might be helpful. 

## Running a Raft simulation

To run the provided in-memory Raft simulation, using 5 nodes, use the following:

```bash
sbt "zio/runMain saft.SaftSim"
```

You'll need to have [sbt](https://www.scala-sbt.org) installed.

## How to read the code

The `zio` module contains a number of files which implement the Raft algorithm, along with an in-memory runnable simulation, an HTTP+json-based app and some tests.

Before getting to know the implementation, it's best to at least skim the [Raft paper](https://raft.github.io/raft.pdf). There's a one-page summary of the algorithm on page 4. 

Reading the files in the following order might be easiest:

1. start with [`domain.scala`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/domain.scala), which defines some basic data types. We've got a representation of a `NodeId`, which simply wraps a number. These ids are then used to identify and communicate between nodes. There are also some opaque types (newtypes), which at runtime are erased to their base representation, however at compile-time are distinct (here we're creating artificial subtypes of `Int` or `String`). This includes `Term`, which counts the Raft terms, log indexes and log data. Finally, there are data structures which represent the content of the logs, such as `LogEntry`, which combines data in the log with the term in which the entry was added (as described in the Raft paper).
2. these basic types are used to create representations of server state in [`state.scala`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/state.scala). This file contains classes such as `ServerState`, `LeaderState` etc., which correspond directly to the state that each server should store as described in the Raft paper. If there's anything extra, it is clearly commented with justification. The state classes contain methods which either update the state with new data (such as appending a new entry), check conditions (such as checking if a log entry can be applied given the current state), or compute views of the data (such as computing the commit index). These functions are always pure, that is side-effect-free, and in case of updates, return a new immutable representation of the state. 
3. then, take a look at [`messages.scala`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/messages.scala). There, you can find the messages that are defined in the Raft protocol, such as `RequestVote` or `AppendEntries`, along with classes representing responses to these requests. The messages are classified basing on whether a client or a sever is the sender/recipient, and if the  message is a request or response. These classification traits (`ToServerMessage`, `ResponseMessage` etc.) are then used to ensure that a message can only be used in appropriate context.
4. the implementation is event-driven, that is appropriate logic is run in response to events being placed on a queue. The events are defined in [`ServerEvent`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/ServerEvent.scala), and the queues are abstracted using the [`Comms`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/Comms.scala) interface. The events are pretty straightforward, representing the possibility that a node can either receive a timeout (for example an election timeout, if there was no communication from the leader for the specified amount of time), or a request/response.
5. finally, with these prerequisites, it should be enough to study the [`Node`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/Node.scala) implementation itself. That's where the main logic of the Raft algorithm is implemented. The code is commented using excerpts from Raft "cheat-sheet", the one-page summary from the [Raft paper, page 4](https://raft.github.io/raft.pdf). The implementation of a node reads events from the queue in a loop, first matching on the event type (timeout / request received / response received), and then matching on the current role of the node (follower / candidate / leader). Each handler follows roughly the same steps: first the state is changed, yielding a new state instance (e.g. `ServerState` or `LeaderState`). Then, side-effects are run, such as restarting the timer, or sending out messages to other nodes (requesting votes or appending entries). After a handler completes, the state is persisted and the response is sent back, if any.
6. to see the implementation in action, it's easiest to run the [`SaftSim`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/SaftSim.scala) app, which runs a number of nodes in-memory, using in-memory communication and in-memory persistence. Using the provided console interface new entries can be added, nodes can be interrupted and started again. Alternatively, you can start an http+json-based version, using [`SaftHttp`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/SaftHttp.scala). This will require starting the three nodes separately, though.
7. there are three files which we haven't yet discussed, though they should be self-explanatory. [`Conf`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/Conf.scala) groups configuration values such as the number of nodes and timeouts. The [`Persistence`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/Persistence.scala) interface is used to save the part of `ServerState` that should be persistent. Finally, [`StateMachine`](https://github.com/softwaremill/saft/blob/master/zio/src/main/scala/saft/StateMachine.scala) is where replicated, committed log entries are applied. A simple implementation applying the given function in the background (so that applying a log entry doesn't block the node itself) is provided.
8. the [test code](https://github.com/softwaremill/saft/blob/master/zio/src/test/scala/saft/NodeTest.scala) might also be interesting, as it uses an manually-managed clock, allowing running time-based tests in a faster and predictable way, eliminating possible flakiness

## Vocabulary

* `node` - one server running the Raft protocol
* `message` - sent between server nodes, or between a client and a server. Includes the messages defined by the Raft protocol: `RequestVote`, `AppendEntries`, `NewEntry`
* `event` - processed by nodes sequentially, mediated by an event queue. Events include outside communication (receiving a `message` as a request or response), or a timeout (e.g. election timeout)
* `state` - the persistent and volatile data associated with each node `role`, as defined in the Raft paper, along with some implementation-specific elements. Includes for example the log itself (of which entries are being replicated), the commit index, or the known leader id.
* `role` - follower, candidate or leader; determines the logic that a particular node runs in response to incoming `event`s.
* `state machine` - where the replicated and committed log entries are applied; the ultimate destination of all log entries 