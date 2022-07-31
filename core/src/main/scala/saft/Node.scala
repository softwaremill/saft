package saft

import zio.*
import com.softwaremill.quicklens.*

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
sealed trait ToServerMessage extends Message
sealed trait FromServerMessage extends Message:
  def term: Term
sealed trait ToClientMessage extends Message
sealed trait ResponseMessage extends Message

case class RequestVote(term: Term, candidateId: NodeId, lastLog: Option[LogEntryTerm]) extends ToServerMessage with FromServerMessage
case class RequestVoteResponse(term: Term, voteGranted: Boolean) extends ResponseMessage with ToServerMessage with FromServerMessage

case class AppendEntries(term: Term, leaderId: NodeId, prevLog: Option[LogEntryTerm], entries: Vector[LogEntry], leaderCommit: LogIndex)
    extends ToServerMessage
    with FromServerMessage
case class AppendEntriesResponse(term: Term, success: Boolean) extends ResponseMessage with ToServerMessage with FromServerMessage

case class NewEntry(entry: LogEntry) extends ToServerMessage
case object NewEntryAddedResponse extends ResponseMessage with ToClientMessage
case class RedirectToLeaderResponse(leaderId: Option[NodeId]) extends ResponseMessage with ToClientMessage

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

  def incrementTerm(self: NodeId): ServerState = copy(currentTerm = Term(currentTerm + 1), votedFor = Some(self))

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
case class CandidateState(receivedVotes: Int)

//

sealed trait ServerEvent
case object Timeout extends ServerEvent
case class RequestReceived(message: ToServerMessage, respond: ResponseMessage => UIO[Unit]) extends ServerEvent

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

class Node(
    nodeId: NodeId,
    events: Queue[ServerEvent],
    send: (NodeId, ToServerMessage) => UIO[Unit],
    stateMachine: StateMachine,
    nodes: Set[NodeId],
    electionTimeout: UIO[Unit]
) {
  private val otherNodes = nodes - nodeId
  private val majority = Math.ceilDiv(nodes.size, 2)

  def start(state: ServerState): UIO[Unit] = ZIO.logAnnotate(NodeIdLogAnnotation, nodeId.id) {
    Timer(electionTimeout, events).flatMap(timer => follower(state, FollowerState(None), timer))
  }

  private def follower(state: ServerState, followerState: FollowerState, electionTimer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "follower") {
      nextEvent(state, electionTimer)(handleFollower(_, state, followerState, electionTimer))
    }

  private def handleFollower(event: ServerEvent, state: ServerState, followerState: FollowerState, electionTimer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "follower") {
      event match {
        case Timeout => startCandidate(state, electionTimer)

        case RequestReceived(rv: RequestVote, respond) =>
          val (response, state2) = {
            if ((rv.term < state.currentTerm) || state.votedForOtherThan(rv.candidateId) || state.hasEntriesAfter(rv.lastLog)) {
              (RequestVoteResponse(state.currentTerm, voteGranted = false), state)
            } else {
              (RequestVoteResponse(state.currentTerm, voteGranted = true), state.copy(votedFor = Some(rv.candidateId)))
            }
          }

          electionTimer.restart.flatMap(newElectionTimer => respond(response) *> follower(state2, followerState, newElectionTimer))

        case RequestReceived(ae: AppendEntries, respond) =>
          val (response, state2) = if (ae.term < state.currentTerm || !state.hasEntryAtTerm(ae.prevLog)) {
            (AppendEntriesResponse(state.currentTerm, success = false), state)
          } else {
            (
              AppendEntriesResponse(state.currentTerm, success = true),
              state.appendEntries(ae.entries, ae.prevLog.map(_.index)).updateCommitIndex(ae.leaderCommit)
            )
          }

          ZIO.foreach(state2.indexesToApply)(i => stateMachine(state2.log(i))).flatMap { _ =>
            val state4 = state2.updateLastAppliedToCommit()

            electionTimer.restart
              .flatMap(newElectionTimer =>
                respond(response) *> follower(state4, followerState.copy(leaderId = Some(ae.leaderId)), newElectionTimer)
              )
          }

        case RequestReceived(_: NewEntry, respond) => respond(RedirectToLeaderResponse(followerState.leaderId))

        // ignore
        case RequestReceived(_: RequestVoteResponse, _)   => ZIO.unit
        case RequestReceived(_: AppendEntriesResponse, _) => ZIO.unit
      }
    }

  private def startCandidate(state: ServerState, electionTimer: Timer): UIO[Unit] = ZIO.logAnnotate(StateLogAnnotation, "candidate-start") {
    val state2 = state.incrementTerm(nodeId)
    electionTimer.restart.flatMap(newElectionTimer =>
      ZIO.foreach(otherNodes)(otherNodeId => send(otherNodeId, RequestVote(state2.currentTerm, nodeId, state2.lastEntryTerm))) *>
        candidate(state2, CandidateState(1), newElectionTimer)
    )
  }

  private def candidate(state: ServerState, candidateState: CandidateState, electionTimer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "candidate") {
      nextEvent(state, electionTimer) {
        case Timeout => startCandidate(state, electionTimer)

        case r @ RequestReceived(rv: RequestVote, respond) =>
          respond(RequestVoteResponse(state.currentTerm, voteGranted = false)) *> candidate(state, candidateState, electionTimer)

        case r @ RequestReceived(ae: AppendEntries, respond) =>
          if state.currentTerm == ae.term
          then handleFollower(r, state, FollowerState(Some(ae.leaderId)), electionTimer)
          else respond(AppendEntriesResponse(state.currentTerm, success = false)) *> candidate(state, candidateState, electionTimer)

        case RequestReceived(_: NewEntry, respond) => respond(RedirectToLeaderResponse(None))
        case RequestReceived(RequestVoteResponse(_, voteGranted), _) if voteGranted =>
          val candidateState2 = candidateState.modify(_.receivedVotes).using(_ + 1)
          if candidateState2.receivedVotes > majority
          then leader(state, electionTimer)
          else candidate(state, candidateState2, electionTimer)
        case RequestReceived(_: RequestVoteResponse, _) => ZIO.unit

        // ignore
        case RequestReceived(_: AppendEntriesResponse, _) => ZIO.unit
      }
    }

  private def leader(state: ServerState, electionTimer: Timer): UIO[Unit] = ZIO.logAnnotate(StateLogAnnotation, "leader") {
    ZIO.log("X") *> ???
  }

  private def nextEvent(state: ServerState, electionTimer: Timer)(next: ServerEvent => UIO[Unit]): UIO[Unit] =
    events.take.flatMap {
      case e @ RequestReceived(msg: FromServerMessage, _) if msg.term > state.currentTerm =>
        handleFollower(e, state.updateTerm(msg.term), FollowerState(None), electionTimer)
      case e => next(e)
    }
}

object Saft extends ZIOAppDefault with Logging {
  def run: Task[Unit] = {
    val numberOfNodes = 5
    val electionTimeoutDuration = Duration.fromMillis(2000)
    val electionRandomization = 500

    val nodes = (1 to numberOfNodes).map(i => NodeId(s"node$i"))
    for {
      eventQueues <- ZIO.foreach(nodes)(nodeId => Queue.unbounded[ServerEvent].map(addLogging).map(nodeId -> _)).map(_.toMap)
      stateMachines <- ZIO
        .foreach(nodes)(nodeId => StateMachine(logEntry => Console.printLine(s"[$nodeId] APPLY: $logEntry").orDie).map(nodeId -> _))
        .map(_.toMap)
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
      electionTimeout = ZIO.random
        .flatMap(_.nextIntBounded(electionRandomization))
        .flatMap(randomization => ZIO.sleep(electionTimeoutDuration.plusMillis(randomization)))
      initial = nodes.map(nodeId =>
        new Node(nodeId, eventQueues(nodeId), send(nodeId), stateMachines(nodeId), nodes.toSet, electionTimeout)
      )
      fibers <- ZIO.foreach(initial)(_.start(ServerState(Vector.empty, None, Term(0), None, None)).fork)
      _ <- ZIO.log(s"$numberOfNodes nodes started. Press any key to exit.")
      _ <- Console.readLine
      _ <- ZIO.foreach(fibers)(f => f.interrupt *> f.join)
      _ <- ZIO.log("Bye!")
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
      case RequestReceived(message, respond) =>
        ZIO.log(s"Request: $message").as {
          RequestReceived(message, response => ZIO.log(s"Response: $response (for: $message)") *> respond(response))
        }

}
