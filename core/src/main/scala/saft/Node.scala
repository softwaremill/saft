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

class Timer(timeout: UIO[Unit], queue: Enqueue[Timeout.type], currentTimer: Fiber.Runtime[Nothing, Boolean]) {
  def restart: UIO[Timer] = currentTimer.interrupt *> (timeout *> queue.offer(Timeout)).fork.map(new Timer(timeout, queue, _))
}

object Timer {
  def apply(timeout: UIO[Unit], queue: Enqueue[Timeout.type]): UIO[Timer] =
    for {
      initialFiber <- ZIO.never.fork
      initialTimer = new Timer(timeout, queue, initialFiber)
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

val StateLogAnnotation = "state"

class Node(
    nodeId: NodeId,
    requests: Queue[ServerEvent],
    send: (NodeId, ServerMessage) => UIO[Unit],
    stateMachine: StateMachine,
    nodes: Set[NodeId],
    electionTimeout: UIO[Unit]
) {
  def start(state: ServerState): UIO[Unit] = Timer(electionTimeout, requests).flatMap(timer => follower(state, FollowerState(None), timer))

  private def follower(state: ServerState, followerState: FollowerState, electionTimer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "follower") {
      requests.take.flatMap {
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

  private def candidate(state: ServerState, timer: Timer): UIO[Unit] = ???
}

object Saft extends ZIOAppDefault with Logging {
  def run: Task[Unit] = {
    val numberOfNodes = 5
    val electionTimeoutDuration = Duration.fromMillis(1000)
    val electionRandomization = 1000

    val nodes = (1 to numberOfNodes).map(i => NodeId(s"node$i"))
    for {
      queues <- ZIO.foreach(nodes)(nodeId => Queue.unbounded[ServerEvent].map(addLogging).map(nodeId -> _)).map(_.toMap)
      stateMachines <- ZIO
        .foreach(nodes)(nodeId => StateMachine(logEntry => Console.printLine(s"[$nodeId] APPLY: $logEntry").orDie).map(nodeId -> _))
        .map(_.toMap)
      send = {
        def doSend(nodeId: NodeId)(toNodeId: NodeId, msg: Message): UIO[Unit] =
          queues(toNodeId).offer(RequestReceived(msg, rspMsg => doSend(toNodeId)(nodeId, rspMsg))).unit
        doSend _
      }
      electionTimeout = ZIO.random
        .flatMap(_.nextIntBounded(electionRandomization))
        .flatMap(randomization => ZIO.sleep(electionTimeoutDuration.plusMillis(randomization)))
      initial = nodes.map(nodeId => new Node(nodeId, queues(nodeId), send(nodeId), stateMachines(nodeId), nodes.toSet, electionTimeout))
      fibers <- ZIO.foreach(initial)(_.start(ServerState(Vector.empty, None, Term(1), None, None)).fork)
      _ <- Console.printLine(s"$numberOfNodes nodes started. Press any key to exit.")
      _ <- Console.readLine
      _ <- ZIO.foreach(fibers)(f => f.interrupt *> f.join)
      _ <- Console.printLine("Bye!")
    } yield ()
  }

  private def addLogging(queue: Queue[ServerEvent]): Queue[ServerEvent] = new Queue[ServerEvent]:
    override def awaitShutdown(implicit trace: Trace): UIO[Unit] = queue.awaitShutdown
    override def capacity: Int = queue.capacity
    override def isShutdown(implicit trace: Trace): UIO[Boolean] = queue.isShutdown
    override def offer(a: ServerEvent)(implicit trace: Trace): UIO[Boolean] = queue.offer(a)
    override def offerAll[A1 <: ServerEvent](as: Iterable[A1])(implicit trace: Trace): UIO[Chunk[A1]] = queue.offerAll(as)
    override def shutdown(implicit trace: Trace): UIO[Unit] = queue.shutdown
    override def size(implicit trace: Trace): UIO[Int] = queue.size
    override def take(implicit trace: Trace): UIO[ServerEvent] = queue.take.flatMap(log)
    override def takeAll(implicit trace: Trace): UIO[Chunk[ServerEvent]] = queue.takeAll.flatMap(ses => ZIO.foreach(ses)(log))
    override def takeUpTo(max: Int)(implicit trace: Trace): UIO[Chunk[ServerEvent]] =
      queue.takeUpTo(max).flatMap(ses => ZIO.foreach(ses)(log))

    private def log(se: ServerEvent): UIO[ServerEvent] = se match
      case Timeout => ZIO.log("Timeout").as(se)
      case request @ RequestReceived(message, respond) =>
        ZIO.succeed(RequestReceived(message, response => ZIO.log(s"Request: $request, response: $response") *> respond(response)))

}
