package saft

import zio.*

import java.io.IOException

/** A Raft simulation using a number of in-memory nodes with in-memory persistence and in-memory communication. */
object SaftSim extends ZIOAppDefault with Logging {
  override def run: Task[Unit] = {
    // configuration
    val numberOfNodes = 5
    val electionTimeoutDuration = Duration.fromMillis(2000)
    val heartbeatTimeoutDuration = Duration.fromMillis(500)
    val electionRandomization = 500
    val applyLogData = (nodeId: NodeId) => (data: LogData) => ZIO.logAnnotate(NodeIdLogAnnotation, nodeId.id)(ZIO.log(s"Apply: $data"))

    // setup nodes
    val nodeIds = (1 to numberOfNodes).map(nodeIdWithIndex)
    val electionTimeout = ZIO.random
      .flatMap(_.nextIntBounded(electionRandomization))
      .flatMap(randomization => ZIO.sleep(electionTimeoutDuration.plusMillis(randomization)))
      .as(Timeout)
    val heartbeatTimeout = ZIO.sleep(heartbeatTimeoutDuration).as(Timeout)

    for {
      comms <- InMemoryComms(nodeIds)
      stateMachines <- ZIO.foreach(nodeIds)(nodeId => StateMachine.background(applyLogData(nodeId)).map(nodeId -> _)).map(_.toMap)
      persistence <- InMemoryPersistence(nodeIds)
      nodes = nodeIds.toList
        .map(nodeId =>
          nodeId -> new Node(
            nodeId,
            comms(nodeId),
            stateMachines(nodeId),
            nodeIds.toSet,
            electionTimeout,
            heartbeatTimeout,
            persistence.forNodeId(nodeId)
          )
        )
        .toMap
      _ <- ZIO.log("Welcome to SaftSim - Scala Raft simulation. Available commands:")
      _ <- ZIO.log("E - exit; Nn data - send new entry <data> to node <n>; Kn - kill node n; Sn - start node n")
      // run interactive loop
      _ <- handleCommands(nodes, comms)
    } yield ()
  }

  private def nodeIdWithIndex(i: Int): NodeId = NodeId(s"node$i")

  private case class RunDone()

  private def handleCommands(
      nodes: Map[NodeId, Node],
      comms: Map[NodeId, InMemoryComms]
  ): IO[IOException, RunDone] =
    val newEntryPattern = "N(\\d+) (.+)".r
    val killPattern = "K(\\d+)".r
    val startPattern = "S(\\d+)".r

    def handleNextCommand(fibers: Map[NodeId, Fiber.Runtime[Nothing, Unit]]): IO[IOException, RunDone] =
      Console.readLine.flatMap {
        case "E" => ZIO.foreach(fibers.values)(f => f.interrupt) *> ZIO.log("Bye!") *> ZIO.succeed(RunDone())

        case newEntryPattern(nodeNumber, data) =>
          val nodeId = nodeIdWithIndex(nodeNumber.toInt)
          comms.get(nodeId) match
            case None => ZIO.log(s"Unknown node: $nodeNumber") *> handleNextCommand(fibers)
            case Some(comm) =>
              comm
                .add(RequestReceived(NewEntry(LogData(data)), responseMessage => ZIO.log(s"Response: $responseMessage")))
                .unit *> handleNextCommand(fibers)

        case killPattern(nodeNumber) =>
          val nodeId = nodeIdWithIndex(nodeNumber.toInt)
          fibers.get(nodeId) match
            case None        => ZIO.log(s"Node $nodeNumber is not started") *> handleNextCommand(fibers)
            case Some(fiber) => fiber.interrupt *> handleNextCommand(fibers.removed(nodeId))

        case startPattern(nodeNumber) =>
          val nodeId = nodeIdWithIndex(nodeNumber.toInt)
          (fibers.get(nodeId), nodes.get(nodeId)) match
            case (None, Some(node)) =>
              comms(nodeId).drain *> node.start.fork.flatMap(fiber => handleNextCommand(fibers + (nodeId -> fiber)))
            case (_, None)          => ZIO.log(s"Unknown node: $nodeNumber") *> handleNextCommand(fibers)
            case (Some(_), Some(_)) => ZIO.log(s"Node $nodeNumber is already started") *> handleNextCommand(fibers)

        case _ => ZIO.log("Unknown command") *> handleNextCommand(fibers)
      }

    ZIO.foreach(nodes)((nodeId, node) => node.start.fork.map(nodeId -> _)).flatMap(handleNextCommand)
}

class InMemoryComms(nodeId: NodeId, eventQueues: Map[NodeId, Queue[ServerEvent]]) extends Comms:
  private val eventQueue = eventQueues(nodeId)

  override def nextEvent: UIO[ServerEvent] = eventQueue.take

  override def send(toNodeId: NodeId, msg: ToServerMessage): UIO[Unit] = eventQueues(toNodeId)
    .offer(
      RequestReceived(
        msg,
        {
          case serverRspMsg: ToServerMessage => new InMemoryComms(toNodeId, eventQueues).send(nodeId, serverRspMsg)
          case _: ToClientMessage            => ZIO.unit // ignore, as inter-node communication doesn't use client messages
        }
      )
    )
    .unit

  override def add(event: ServerEvent): UIO[Unit] = eventQueue.offer(event).unit

  def drain: UIO[Unit] = eventQueue.takeAll.unit

object InMemoryComms:
  def apply(nodeIds: Seq[NodeId]): UIO[Map[NodeId, InMemoryComms]] =
    ZIO.foreach(nodeIds)(nodeId => Queue.sliding[ServerEvent](16).map(nodeId -> _)).map(_.toMap).map { eventQueues =>
      nodeIds.map(nodeId => nodeId -> new InMemoryComms(nodeId, eventQueues)).toMap
    }

class InMemoryPersistence(refs: Map[NodeId, Ref[ServerState]]):
  def forNodeId(nodeId: NodeId): Persistence = new Persistence {
    private val ref = refs(nodeId)
    override def apply(oldState: ServerState, newState: ServerState): UIO[Unit] =
      ref.set(newState.copy(commitIndex = None, lastApplied = None))
    override def get: UIO[ServerState] = ref.get
  }

object InMemoryPersistence:
  def apply(nodeIds: Seq[NodeId]): UIO[InMemoryPersistence] =
    ZIO.foreach(nodeIds.toList)(nodeId => Ref.make(ServerState.Initial).map(nodeId -> _)).map(_.toMap).map(new InMemoryPersistence(_))
