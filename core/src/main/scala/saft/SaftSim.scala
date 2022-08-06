package saft

import zio.*

import java.io.IOException

object SaftSim extends ZIOAppDefault with Logging {
  def run: Task[Unit] = {
    // configuration
    val numberOfNodes = 5
    val electionTimeoutDuration = Duration.fromMillis(2000)
    val heartbeatTimeoutDuration = Duration.fromMillis(500)
    val electionRandomization = 500
    val applyLogData = (nodeId: NodeId) => (data: LogData) => ZIO.logAnnotate(NodeIdLogAnnotation, nodeId.id)(ZIO.log(s"Apply: $data"))

    // setup nodes
    val nodeIds = (1 to numberOfNodes).map(nodeIdWithIndex)
    for {
      eventQueues <- ZIO.foreach(nodeIds)(nodeId => Queue.sliding[ServerEvent](16).map(nodeId -> _)).map(_.toMap)
      send = {
        def doSend(nodeId: NodeId)(toNodeId: NodeId, msg: ToServerMessage): UIO[Unit] =
          eventQueues(toNodeId)
            .offer(
              RequestReceived(
                msg,
                {
                  case serverRspMsg: ToServerMessage => doSend(toNodeId)(nodeId, serverRspMsg)
                  case _: ToClientMessage            => ZIO.unit // ignore
                }
              )
            )
            .unit
        doSend _
      }
      stateMachines <- ZIO.foreach(nodeIds)(nodeId => StateMachine.background(applyLogData(nodeId)).map(nodeId -> _)).map(_.toMap)
      electionTimeout = ZIO.random
        .flatMap(_.nextIntBounded(electionRandomization))
        .flatMap(randomization => ZIO.sleep(electionTimeoutDuration.plusMillis(randomization)))
      heartbeatTimeout = ZIO.sleep(heartbeatTimeoutDuration)
      persistence <- InMemoryPersistence(nodeIds)
      nodes = nodeIds.toList
        .map(nodeId =>
          nodeId -> new Node(
            nodeId,
            eventQueues(nodeId),
            send(nodeId),
            stateMachines(nodeId),
            nodeIds.toSet,
            electionTimeout,
            heartbeatTimeout,
            persistence.forNodeId(nodeId)
          )
        )
        .toMap
      _ <- ZIO.log("E - exit; Nn data - send new entry <data> to node <n>; Kn - kill node n; Sn - start node n")
      // run interactive loop
      _ <- run(nodes, eventQueues)
    } yield ()
  }

  private def nodeIdWithIndex(i: Int): NodeId = NodeId(s"node$i")

  private case class RunDone()

  private def run(
      nodes: Map[NodeId, Node],
      queues: Map[NodeId, Queue[ServerEvent]]
  ): IO[IOException, RunDone] =
    val newEntryPattern = "N(\\d+) (.+)".r
    val killPattern = "K(\\d+)".r
    val startPattern = "S(\\d+)".r

    def doRun(fibers: Map[NodeId, Fiber.Runtime[Nothing, Unit]]): IO[IOException, RunDone] =
      Console.readLine.flatMap {
        case "E" => ZIO.foreach(fibers.values)(f => f.interrupt) *> ZIO.log("Bye!") *> ZIO.succeed(RunDone())

        case newEntryPattern(nodeNumber, data) =>
          val nodeId = nodeIdWithIndex(nodeNumber.toInt)
          queues.get(nodeId) match
            case None => ZIO.log(s"Unknown node: $nodeNumber") *> doRun(fibers)
            case Some(queue) =>
              queue
                .offer(RequestReceived(NewEntry(LogData(data)), responseMessage => ZIO.log(s"Response: $responseMessage")))
                .unit *> doRun(fibers)

        case killPattern(nodeNumber) =>
          val nodeId = nodeIdWithIndex(nodeNumber.toInt)
          fibers.get(nodeId) match
            case None        => ZIO.log(s"Node $nodeNumber is not started") *> doRun(fibers)
            case Some(fiber) => fiber.interrupt *> doRun(fibers.removed(nodeId))

        case startPattern(nodeNumber) =>
          val nodeId = nodeIdWithIndex(nodeNumber.toInt)
          (fibers.get(nodeId), nodes.get(nodeId)) match
            case (None, Some(node)) => queues(nodeId).takeAll *> node.start.fork.flatMap(fiber => doRun(fibers + (nodeId -> fiber)))
            case (_, None)          => ZIO.log(s"Unknown node: $nodeNumber") *> doRun(fibers)
            case (Some(_), Some(_)) => ZIO.log(s"Node $nodeNumber is already started") *> doRun(fibers)

        case _ => ZIO.log("Unknown command") *> doRun(fibers)
      }

    ZIO.foreach(nodes)((nodeId, node) => node.start.fork.map(nodeId -> _)).flatMap(doRun)
}
