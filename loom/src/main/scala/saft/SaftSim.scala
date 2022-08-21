package saft

import com.typesafe.scalalogging.StrictLogging
import Logging.setNodeLogAnnotation

import java.io.IOException
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.io.StdIn

/** A Raft simulation using a number of in-memory nodes with in-memory persistence and in-memory communication. */
object SaftSim extends StrictLogging:
  def main(args: Array[String]): Unit =
    // configuration
    val conf = Conf.default(5)
    val applyLogData = (nodeId: NodeId) =>
      (data: LogData) => {
        setNodeLogAnnotation(nodeId)
        logger.info(s"Apply: $data")
      }

    // setup nodes
    val loom = new Loom()
    val comms = InMemoryComms(conf.nodeIds)
    val stateMachines = conf.nodeIds.map(nodeId => nodeId -> StateMachine.background(loom, applyLogData(nodeId))).toMap
    val persistence = InMemoryPersistence(conf.nodeIds)
    val nodes = conf.nodeIds.toList
      .map(nodeId => nodeId -> new Node(nodeId, comms(nodeId), stateMachines(nodeId), conf, persistence.forNodeId(nodeId)))
      .toMap
    logger.info("Welcome to SaftSim - Scala Raft simulation. Available commands:")
    logger.info("E - exit; Nn data - send new entry <data> to node <n>; Kn - kill node n; Sn - start node n")
    logger.info(s"Configuration: ${conf.show}")
    // run interactive loop
    handleCommands(nodes, comms)
  end main

  private case class RunDone()

  private def handleCommands(
      nodes: Map[NodeId, Node],
      comms: Map[NodeId, InMemoryComms]
  ): RunDone =
    val newEntryPattern = "N(\\d+) (.+)".r
    val killPattern = "K(\\d+)".r
    val startPattern = "S(\\d+)".r

    @tailrec
    def handleNextCommand(fibers: Map[NodeId, Loom]): RunDone =
      StdIn.readLine() match
        case "E" =>
          for (f <- fibers.values) f.close()
          logger.info("Bye!")
          RunDone()

        case newEntryPattern(nodeNumber, data) =>
          val nodeId = NodeId(nodeNumber.toInt)
          comms.get(nodeId) match
            case None =>
              logger.info(s"Unknown node: $nodeNumber")
              handleNextCommand(fibers)
            case Some(comm) =>
              comm.add(ServerEvent.RequestReceived(NewEntry(LogData(data)), responseMessage => logger.info(s"Response: $responseMessage")))
              handleNextCommand(fibers)

        case killPattern(nodeNumber) =>
          val nodeId = NodeId(nodeNumber.toInt)
          fibers.get(nodeId) match
            case None =>
              logger.info(s"Node $nodeNumber is not started")
              handleNextCommand(fibers)
            case Some(fiber) =>
              fiber.close()
              handleNextCommand(fibers.removed(nodeId))

        case startPattern(nodeNumber) =>
          val nodeId = NodeId(nodeNumber.toInt)
          (fibers.get(nodeId), nodes.get(nodeId)) match
            case (None, Some(node)) =>
              comms(nodeId).drain()
              val loom = new Loom()
              node.start(loom)
              handleNextCommand(fibers + (nodeId -> loom))
            case (_, None) =>
              logger.info(s"Unknown node: $nodeNumber")
              handleNextCommand(fibers)
            case (Some(_), Some(_)) =>
              logger.info(s"Node $nodeNumber is already started")
              handleNextCommand(fibers)

        case _ =>
          logger.info("Unknown command")
          handleNextCommand(fibers)
    end handleNextCommand

    val fibers = for ((nodeId, node) <- nodes) yield {
      val loom = new Loom()
      node.start(loom)
      nodeId -> loom
    }
    handleNextCommand(fibers)
  end handleCommands

private class InMemoryComms(nodeId: NodeId, eventQueues: Map[NodeId, BlockingQueue[ServerEvent]]) extends Comms:
  private val eventQueue = eventQueues(nodeId)

  override def next: ServerEvent = eventQueue.take()

  override def send(toNodeId: NodeId, msg: RequestMessage with FromServerMessage): Unit =
    eventQueues(toNodeId)
      .offer(
        ServerEvent.RequestReceived(
          msg,
          {
            case serverMsg: ToServerMessage => eventQueues(nodeId).offer(ServerEvent.ResponseReceived(serverMsg))
            case _                          => () // ignore, as inter-node communication doesn't use client messages
          }
        )
      )

  override def add(event: ServerEvent): Unit = eventQueue.offer(event)

  def drain(): Unit = eventQueue.drainTo(new java.util.ArrayList[ServerEvent]())

private object InMemoryComms:
  def apply(nodeIds: Seq[NodeId]): Map[NodeId, InMemoryComms] =
    val eventQueues = nodeIds.map(nodeId => nodeId -> new ArrayBlockingQueue[ServerEvent](16)).toMap
    nodeIds.map(nodeId => nodeId -> new InMemoryComms(nodeId, eventQueues)).toMap

private class InMemoryPersistence(refs: Map[NodeId, AtomicReference[ServerState]]):
  def forNodeId(nodeId: NodeId): Persistence = new Persistence {
    private val ref = refs(nodeId)
    override def apply(oldState: ServerState, newState: ServerState): Unit =
      ref.set(newState.copy(commitIndex = None, lastApplied = None))
    override def get: ServerState = ref.get
  }

private object InMemoryPersistence:
  def apply(nodeIds: Seq[NodeId]): InMemoryPersistence =
    new InMemoryPersistence(nodeIds.map(nodeId => nodeId -> new AtomicReference[ServerState](ServerState.Initial)).toMap)
