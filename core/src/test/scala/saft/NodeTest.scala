package saft

import zio.*
import zio.test.*

object NodeTest extends ZIOSpecDefault:
  def spec: Spec[Any, Throwable] =
    suite("NodeTest")(
      test("should replicate a single entry") {
        // given
        for {
          fixture <- startNodes(Conf.default(5))
          // when
          _ <- TestClock.adjust(Duration.fromSeconds(5)) // elect leader
          _ <- newEntry(LogData("entry1"), fixture.comms)
          _ <- TestClock.adjust(Duration.fromSeconds(5)) // replicate
          // then
          results <- ZIO.foreach(fixture.conf.nodeIds)(nodeId => fixture.applied(nodeId).get.map(a => assertTrue(a == Vector("entry1"))))
          // finally
          _ <- fixture.interrupt
        } yield results.reduce(_ && _)
      }
    )

  case class TestFixture(
      conf: Conf,
      nodes: Map[NodeId, Node],
      comms: Map[NodeId, Comms],
      fibers: Map[NodeId, Fiber.Runtime[Nothing, Any]],
      applied: Map[NodeId, Ref[Vector[String]]]
  ):
    def interrupt: UIO[Unit] = ZIO.foreachDiscard(conf.nodeIds)(nodeId => fibers(nodeId).interrupt)

  def startNodes(conf: Conf): UIO[TestFixture] =
    for {
      applied <- ZIO.foreach(conf.nodeIds)(nodeId => Ref.make[Vector[String]](Vector.empty).map(nodeId -> _)).map(_.toMap)
      comms <- InMemoryComms(conf.nodeIds)
      stateMachines <- ZIO
        .foreach(conf.nodeIds)(nodeId => StateMachine.background(data => applied(nodeId).update(_ :+ data)).map(nodeId -> _))
        .map(_.toMap)
      persistence <- InMemoryPersistence(conf.nodeIds)
      nodes = conf.nodeIds.toList
        .map(nodeId => nodeId -> new Node(nodeId, comms(nodeId), stateMachines(nodeId), conf, persistence.forNodeId(nodeId)))
        .toMap
      fibers <- ZIO.foreach(nodes)((nodeId, node) => node.start.fork.map(nodeId -> _)).map(_.toMap)
    } yield TestFixture(conf, nodes, comms, fibers, applied)

  /** Trying adding a new entry to each node in turn, until a leader is found. */
  def newEntry(data: LogData, comms: Map[NodeId, Comms]): Task[Unit] =
    def doRun(cs: List[(NodeId, Comms)]): Task[Unit] = cs match
      case Nil => ZIO.fail(new RuntimeException(s"Cannot send new entry $data, no leader"))
      case (n, c) :: tail =>
        request(n, NewEntry(data), c).flatMap {
          case RedirectToLeaderResponse(_)       => doRun(tail)
          case NewEntryAddedSuccessfullyResponse => ZIO.unit
          case r => ZIO.fail(new RuntimeException(s"When sending a new entry request, got unexpected response: $r"))
        }
    doRun(comms.toList)

  def request(toNodeId: NodeId, msg: RequestMessage, comms: Comms): Task[ResponseMessage] = for {
    p <- Promise.make[Nothing, ResponseMessage]
    _ <- comms.add(RequestReceived(msg, p.succeed(_).unit))
    r <- p.await
      .timeoutFail(new RuntimeException(s"Timeout while waiting for a response to $msg sent to $toNodeId"))(
        Duration.fromSeconds(1)
      )
  } yield r
