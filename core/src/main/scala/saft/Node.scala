package saft

import zio.*

case class NodeId(id: String)

opaque type Term <: Int = Int
object Term:
  def apply(t: Int): Term = t

opaque type LogIndex <: Int = Int
object LogIndex:
  def apply(i: Int): LogIndex = i

case class LogEntry(value: String, term: Term)
case class LogEntryTerm(term: Term, index: LogIndex)

//

sealed trait Message
sealed trait ServerMessage extends Message
sealed trait ClientMessage extends Message
sealed trait ResponseMessage extends Message

case class RequestVote(term: Term, candidateId: NodeId, lastLog: Option[LogEntryTerm]) extends ServerMessage
case class RequestVoteResponse(term: Term, voteGranted: Boolean) extends ResponseMessage with ServerMessage

case class AppendEntries(term: Term, leaderId: NodeId, prevLog: Option[LogEntryTerm], entries: Vector[LogEntry], leaderCommit: LogIndex)
    extends ServerMessage
case class AppendEntriesResponse(term: Term, success: Boolean) extends ResponseMessage with ServerMessage

case class NewEntry(entry: LogEntry) extends ClientMessage
case object NewEntryResponse extends ResponseMessage with ClientMessage

case class RedirectToLeaderResponse(leaderId: Option[NodeId]) extends ResponseMessage with ClientMessage

//

case class ServerState(
    log: Vector[LogEntry],
    votedFor: Option[NodeId],
    currentTerm: Term,
    commitIndex: Option[LogIndex],
    lastApplied: Option[LogIndex]
) {
  def updateTerm(observedTerm: Term): ServerState =
    if (observedTerm > currentTerm) copy(currentTerm = observedTerm, votedFor = None) else this

  def lastEntryTerm: Option[LogEntryTerm] = log.lastOption.map(lastLogEntry => LogEntryTerm(lastLogEntry.term, LogIndex(log.length - 1)))

  def votedForOtherThan(candidateId: NodeId): Boolean = votedFor.nonEmpty && !votedFor.contains(candidateId)

  def hasEntriesAfter(t: Option[LogEntryTerm]): Boolean = (t, lastEntryTerm) match
    case (None, None)    => false
    case (None, _)       => true
    case (Some(_), None) => false
    case (Some(otherLastEntry), Some(lastEntry)) =>
      lastEntry.term > otherLastEntry.term || ((lastEntry.term == otherLastEntry.term) && (lastEntry.index > otherLastEntry.index))

  def hasEntryAtTerm(t: Option[LogEntryTerm]): Boolean = t match
    case None               => true
    case Some(logEntryTerm) => log.length > logEntryTerm.index && log(logEntryTerm.index).term == logEntryTerm.term

  def appendEntries(entries: Vector[LogEntry], afterIndex: Option[LogIndex]): ServerState =
    copy(log = log.take(afterIndex.getOrElse(log.length) + 1) ++ entries)

  def updateCommitIndex(index: LogIndex): ServerState = if (commitIndex.forall(_ < index)) copy(commitIndex = Some(index)) else this

  def updateLastAppliedToCommit(): ServerState = copy(lastApplied = commitIndex)

  def indexesToApply: Seq[Int] = (lastApplied, commitIndex) match {
    case (None, None)         => Nil
    case (None, Some(ci))     => 0 to ci
    case (Some(la), Some(ci)) => (la + 1) to ci
    case (Some(la), None)     => throw new IllegalStateException(s"Last applied is set to $la, but no commit index is set")
  }

}
case class LeaderState(nextIndex: Map[NodeId, LogIndex], matchIndex: Map[NodeId, LogIndex])
case class FollowerState(leaderId: Option[NodeId])

//

sealed trait ServerEvent
case object Timeout extends ServerEvent
case class RequestReceived(message: Message, respond: ResponseMessage => UIO[Unit]) extends ServerEvent

//

class Timer[T](timeout: UIO[Unit], queue: Queue[T], t: T, currentTimer: Fiber.Runtime[Nothing, Boolean]) {
  def restart: UIO[Timer[T]] = currentTimer.interrupt *> (timeout *> queue.offer(t)).fork.map(new Timer(timeout, queue, t, _))
  def waitForTimeout: UIO[T] = queue.take
}

object Timer {
  // timeout - randomized
  def apply[T](timeout: UIO[Unit], t: T): UIO[Timer[T]] =
    for {
      queue <- Queue.dropping[T](1)
      initialFiber <- ZIO.never.fork
      initialTimer = new Timer[T](timeout, queue, t, initialFiber)
      restartedTimer <- initialTimer.restart
    } yield restartedTimer
}

//

class StateMachine(toApply: Enqueue[LogEntry]):
  def apply(entry: LogEntry): UIO[Unit] = toApply.offer(entry).unit

object StateMachine:
  def apply(doApply: LogEntry => UIO[Unit]): UIO[StateMachine] =
    for {
      toApplyQueue <- Queue.unbounded[LogEntry]
      _ <- toApplyQueue.take.flatMap(doApply).forever.fork
    } yield new StateMachine(toApplyQueue)

//

class Node(
    nodeId: NodeId,
    receive: UIO[RequestReceived],
    send: (NodeId, ServerMessage) => UIO[Unit],
    stateMachine: StateMachine,
    nodes: Set[NodeId]
) {
  def follower(state: ServerState, followerState: FollowerState, electionTimer: Timer[Timeout.type]): UIO[Unit] = {
    electionTimer.waitForTimeout.raceFirst(receive).flatMap {
      case Timeout => candidate(state, electionTimer)

      case RequestReceived(rv: RequestVote, respond) =>
        val state2 = state.updateTerm(rv.term)

        val (response, state3) = {
          if ((rv.term < state2.currentTerm) || state2.votedForOtherThan(rv.candidateId) || state2.hasEntriesAfter(rv.lastLog)) {
            (RequestVoteResponse(state2.currentTerm, voteGranted = false), state2)
          } else {
            (RequestVoteResponse(state2.currentTerm, voteGranted = true), state2.copy(votedFor = Some(rv.candidateId)))
          }
        }

        electionTimer.restart.flatMap(newElectionTimer => respond(response) *> follower(state3, followerState, newElectionTimer))

      case RequestReceived(ae: AppendEntries, respond) =>
        val state2 = state.updateTerm(ae.term)

        val (response, state3) = if (ae.term < state2.currentTerm || !state2.hasEntryAtTerm(ae.prevLog)) {
          (AppendEntriesResponse(state2.currentTerm, success = false), state2)
        } else {
          (
            AppendEntriesResponse(state2.currentTerm, success = true),
            state2.appendEntries(ae.entries, ae.prevLog.map(_.index)).updateCommitIndex(ae.leaderCommit)
          )
        }

        ZIO.foreach(state3.indexesToApply)(i => stateMachine(state3.log(i))).flatMap { _ =>
          val state4 = state3.updateLastAppliedToCommit()

          electionTimer.restart
            .flatMap(newElectionTimer =>
              respond(response) *> follower(state4, followerState.copy(leaderId = Some(ae.leaderId)), newElectionTimer)
            )
        }

      case RequestReceived(_: NewEntry, respond) => respond(RedirectToLeaderResponse(followerState.leaderId))

      // ignore
      case RequestReceived(_: RequestVoteResponse, _)      => ZIO.unit
      case RequestReceived(_: AppendEntriesResponse, _)    => ZIO.unit
      case RequestReceived(_: NewEntryResponse.type, _)    => ZIO.unit
      case RequestReceived(_: RedirectToLeaderResponse, _) => ZIO.unit
    }
  }

  def candidate(state: ServerState, timer: Timer[Timeout.type]): UIO[Unit] = ???
}

object Saft extends ZIOAppDefault with Logging {
  def run: Task[Unit] = {
    val numberOfNodes = 1
    val electionTimeoutDuration = Duration.fromMillis(5000)
    val electionRandomization = 100

    val nodes = (1 to numberOfNodes).map(i => NodeId(s"node$i"))
    for {
      queues <- ZIO.foreach(nodes)(nodeId => Queue.unbounded[RequestReceived].map(nodeId -> _)).map(_.toMap)
      stateMachines <- ZIO
        .foreach(nodes)(nodeId => StateMachine(logEntry => Console.printLine(s"[$nodeId] APPLY: $logEntry").orDie).map(nodeId -> _))
        .map(_.toMap)
      receive = (nodeId: NodeId) => queues(nodeId).take
      send = {
        def doSend(nodeId: NodeId)(toNodeId: NodeId, msg: Message): UIO[Unit] =
          queues(toNodeId).offer(RequestReceived(msg, rspMsg => doSend(toNodeId)(nodeId, rspMsg))).unit
        doSend _
      }
      initial = nodes.map(nodeId => new Node(nodeId, receive(nodeId), send(nodeId), stateMachines(nodeId), nodes.toSet))
      electionTimeout = ZIO.random
        .flatMap(_.nextIntBounded(electionRandomization))
        .flatMap(randomization => ZIO.sleep(electionTimeoutDuration.plusMillis(randomization)))
      electionTimer <- Timer(electionTimeout, Timeout)
      fibers <- ZIO.foreach(initial)(
        _.follower(ServerState(Vector.empty, None, Term(1), None, None), FollowerState(None), electionTimer).fork
      )
      _ <- Console.printLine(s"$numberOfNodes nodes started. Press any key to exit.")
      _ <- Console.readLine
      _ <- ZIO.foreach(fibers)(f => f.interrupt *> f.join)
      _ <- Console.printLine("Bye!")
    } yield ()
  }

  // override def run: ZIO[Any, Any, Any] = (ZIO.log("abc") *> ZIO.dieMessage("xyz"))

//  override def run: ZIO[Any, Any, Any] =
//    ZIO.dieMessage("xyz").fork.flatMap(f => ZIO.sleep(Duration.fromMillis(100)) *> f.interrupt *> f.join)

  def run3: ZIO[Any, Throwable, Any] = Queue.unbounded[String].flatMap { q =>

    val et = ZIO.random
      .flatMap(_.nextIntBounded(100))
      .flatMap(randomization => ZIO.sleep(Duration.fromMillis(100).plusMillis(randomization)))

    val t = q.take.flatMap(s => Console.printLine(s))
    q.offer("a") *> q.offer("b") *> et.raceFirst(t) *> et.raceFirst(t)
  }
}
