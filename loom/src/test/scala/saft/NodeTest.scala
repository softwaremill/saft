package saft

import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.TimeoutException

class NodeTest extends AnyFunSuite with Matchers:
  private def forward5seconds(): Unit = Thread.sleep(5000)

  test("should replicate a single entry") {
    // given
    val fixture = startNodes(Conf.default(5))
    try {
      // when
      forward5seconds() // elect leader
      newEntry(LogData("entry1"), fixture.comms)
      forward5seconds() // replicate
      // then
      fixture.conf.nodeIds.foreach(nodeId => fixture.applied(nodeId).get shouldBe Vector("entry1"))
    } finally fixture.cancel()
  }

  test("should replicate an entry after the leader is interrupted") {
    // given
    val fixture = startNodes(Conf.default(5))
    try {
      // when
      forward5seconds() // elect leader
      val leader = newEntry(LogData("entry1"), fixture.comms)
      forward5seconds() // replicate
      fixture.fibers(leader).cancel()
      forward5seconds() // elect new leader
      newEntry(LogData("entry2"), fixture.comms)
      forward5seconds() // replicate
      // then
      fixture.applied(leader).get shouldBe Vector("entry1")
      fixture.conf.nodeIds
        .filterNot(_ == leader)
        .foreach(nodeId => fixture.applied(nodeId).get shouldBe Vector("entry1", "entry2"))
    } finally fixture.cancel()
  }

  case class TestFixture(
      conf: Conf,
      nodes: Map[NodeId, Node],
      comms: Map[NodeId, Comms],
      fibers: Map[NodeId, Cancellable],
      applied: Map[NodeId, AtomicReference[Vector[String]]]
  ):
    def cancel(): Unit = conf.nodeIds.foreach(nodeId => fibers(nodeId).cancel())

  def startNodes(conf: Conf): TestFixture =
    val applied = conf.nodeIds.map(nodeId => nodeId -> new AtomicReference[Vector[String]](Vector.empty)).toMap
    val comms = InMemoryComms(conf.nodeIds)
    val stateMachines = conf.nodeIds.map(nodeId => nodeId -> StateMachine.background(data => applied(nodeId).getAndUpdate(_ :+ data))).toMap
    val persistence = InMemoryPersistence(conf.nodeIds)
    val nodes = conf.nodeIds.toList
      .map(nodeId => nodeId -> new Node(nodeId, comms(nodeId), stateMachines(nodeId), conf, persistence.forNodeId(nodeId)))
      .toMap
    val fibers = nodes.map((nodeId, node) => nodeId -> node.start())
    TestFixture(conf, nodes, comms, fibers, applied)

  /** Trying adding a new entry to each node in turn, until a leader is found. */
  def newEntry(data: LogData, comms: Map[NodeId, Comms]): NodeId =
    @tailrec
    def doRun(cs: List[(NodeId, Comms)]): NodeId = cs match
      case Nil => throw new RuntimeException(s"Cannot send new entry $data, no leader")
      case (n, c) :: tail =>
        request(n, NewEntry(data), c) match {
          case RedirectToLeaderResponse(_)       => doRun(tail)
          case NewEntryAddedSuccessfullyResponse => n
          case r => throw new RuntimeException(s"When sending a new entry request, got unexpected response: $r")
        }

    doRun(comms.toList)

  def request(toNodeId: NodeId, msg: RequestMessage, comms: Comms): ResponseMessage =
    val p = new CompletableFuture[ResponseMessage]
    comms.add(ServerEvent.RequestReceived(msg, p.complete))
    try p.get(1, TimeUnit.SECONDS)
    catch case e: TimeoutException => throw new RuntimeException(s"Timeout while waiting for a response to $msg sent to $toNodeId", e)
