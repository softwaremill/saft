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
case class LogIndexTerm(term: Term, index: LogIndex)

//

sealed trait Message
sealed trait ToServerMessage extends Message
sealed trait FromServerMessage extends Message:
  def term: Term
sealed trait ToClientMessage extends Message
sealed trait ResponseMessage extends Message

case class RequestVote(term: Term, candidateId: NodeId, lastLog: Option[LogIndexTerm]) extends ToServerMessage with FromServerMessage
case class RequestVoteResponse(term: Term, voteGranted: Boolean) extends ResponseMessage with ToServerMessage with FromServerMessage

case class AppendEntries(
    term: Term,
    leaderId: NodeId,
    prevLog: Option[LogIndexTerm],
    entries: Vector[LogEntry],
    leaderCommit: Option[LogIndex]
) extends ToServerMessage
    with FromServerMessage
// the extra followerId and entryIndexRange are needed to correlate the response & request, so that the leader can update nextIndex appropriately
case class AppendEntriesResponse(term: Term, success: Boolean, followerId: NodeId, entryIndexRange: Option[(LogIndex, LogIndex)])
    extends ResponseMessage
    with ToServerMessage
    with FromServerMessage
object AppendEntriesResponse:
  def to(followerId: NodeId, appendEntries: AppendEntries)(term: Term, success: Boolean): AppendEntriesResponse =
    AppendEntriesResponse(
      term,
      success,
      followerId, {
        val firstIndex = appendEntries.entries.headOption.map(_ => LogIndex(appendEntries.prevLog.map(_.index).getOrElse(-1) + 1))
        firstIndex.map(f => (f, LogIndex(f + appendEntries.entries.length - 1)))
      }
    )

case class NewEntry(entry: String) extends ToServerMessage
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

  def lastEntryTerm: Option[LogIndexTerm] = log.lastOption.map(lastLogEntry => LogIndexTerm(lastLogEntry.term, LogIndex(log.length - 1)))

  def votedForOtherThan(candidateId: NodeId): Boolean = votedFor.nonEmpty && !votedFor.contains(candidateId)

  def hasEntriesAfter(t: Option[LogIndexTerm]): Boolean = (t, lastEntryTerm) match
    case (None, None)    => false
    case (None, _)       => true
    case (Some(_), None) => false
    case (Some(otherLastEntry), Some(lastEntry)) =>
      lastEntry.term > otherLastEntry.term || ((lastEntry.term == otherLastEntry.term) && (lastEntry.index > otherLastEntry.index))

  def hasEntryAtTerm(t: Option[LogIndexTerm]): Boolean = t match
    case None               => true
    case Some(logEntryTerm) => log.length > logEntryTerm.index && log(logEntryTerm.index).term == logEntryTerm.term

  def appendEntry(entry: LogEntry): ServerState = copy(log = log :+ entry)

  def appendEntries(entries: Vector[LogEntry], afterIndex: Option[LogIndex]): ServerState =
    copy(log = log.take(afterIndex.getOrElse(-1) + 1) ++ entries)

  def updateCommitIndex(leaderCommitIndex: Option[LogIndex]): ServerState = (commitIndex, leaderCommitIndex) match
    case (Some(ours), Some(leader)) if ours < leader => copy(commitIndex = leaderCommitIndex)
    case (None, Some(_))                             => copy(commitIndex = leaderCommitIndex)
    case _                                           => this

  def updateLastAppliedToCommit(): ServerState = copy(lastApplied = commitIndex)

  def indexesToApply: Seq[Int] = (lastApplied, commitIndex) match {
    case (None, None)         => Nil
    case (None, Some(ci))     => 0 to ci
    case (Some(la), Some(ci)) => (la + 1) to ci
    case (Some(la), None)     => throw new IllegalStateException(s"Last applied is set to $la, but no commit index is set")
  }
}

// the extra awaitingResponse is needed to properly reply to NewEntry requests once entries are replicated
case class LeaderState(
    nextIndex: Map[NodeId, LogIndex],
    matchIndex: Map[NodeId, Option[LogIndex]],
    awaitingResponses: Vector[(LogIndex, UIO[Unit])]
):
  def appendSuccessful(nodeId: NodeId, lastIndex: Option[LogIndex]): LeaderState = lastIndex match
    case Some(last) =>
      LeaderState(
        nextIndex.updated(nodeId, LogIndex(math.max(nextIndex(nodeId), last + 1))),
        matchIndex.updated(nodeId, Some(LogIndex(math.max(matchIndex(nodeId).getOrElse(-1), last)))),
        awaitingResponses
      )
    case None => this

  def appendFailed(nodeId: NodeId, firstIndex: Option[LogIndex]): LeaderState = firstIndex match
    case Some(first) =>
      LeaderState(
        nextIndex.updated(nodeId, LogIndex(math.min(nextIndex(nodeId), first - 1))),
        matchIndex,
        awaitingResponses
      )
    case None => this

  def commitIndex(ourIndex: Option[LogIndex], majority: Int): Option[LogIndex] =
    val indexes = (ourIndex :: matchIndex.values.toList).flatten
    indexes.filter(i => indexes.count(_ >= i) >= majority).maxOption

  def addAwaitingResponse(index: LogIndex, respond: UIO[Unit]): LeaderState =
    copy(awaitingResponses = awaitingResponses :+ (index, respond))

  def removeAwaitingResponses(upToIndex: LogIndex): (LeaderState, Vector[UIO[Unit]]) =
    val (doneWaiting, stillAwaiting) = awaitingResponses.span(_._1 <= upToIndex)
    (copy(awaitingResponses = stillAwaiting), doneWaiting.map(_._2))

case class FollowerState(leaderId: Option[NodeId])
case class CandidateState(receivedVotes: Int)

//

sealed trait ServerEvent
case object Timeout extends ServerEvent
case class RequestReceived(message: ToServerMessage, respond: ResponseMessage => UIO[Unit]) extends ServerEvent

//

class Timer(
    electionTimeout: UIO[Unit],
    heartbeatTimeout: UIO[Unit],
    queue: Enqueue[Timeout.type],
    currentTimer: Fiber.Runtime[Nothing, Boolean]
) {
  private def restart(timeout: UIO[Unit]): UIO[Timer] =
    currentTimer.interrupt *> (timeout *> queue.offer(Timeout)).fork.map(new Timer(electionTimeout, heartbeatTimeout, queue, _))
  def restartElection: UIO[Timer] = restart(electionTimeout)
  def restartHeartbeat: UIO[Timer] = restart(heartbeatTimeout)
}

object Timer {
  def apply(electionTimeout: UIO[Unit], heartbeatTimeout: UIO[Unit], queue: Enqueue[Timeout.type]): UIO[Timer] =
    ZIO.never.fork.map(new Timer(electionTimeout, heartbeatTimeout, queue, _))
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
    electionTimeout: UIO[Unit],
    heartbeatTimeout: UIO[Unit]
) {
  private val otherNodes = nodes - nodeId
  private val majority = math.ceil(nodes.size.toDouble / 2).toInt

  def start(state: ServerState): UIO[Unit] = ZIO.logAnnotate(NodeIdLogAnnotation, nodeId.id) {
    Timer(electionTimeout, heartbeatTimeout, events)
      .flatMap(_.restartElection)
      .flatMap(timer => follower(state, FollowerState(None), timer))
  }

  private def follower(state: ServerState, followerState: FollowerState, timer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "follower") {
      nextEvent(state, timer)(handleFollower(_, state, followerState, timer))
    }

  private def handleFollower(event: ServerEvent, state: ServerState, followerState: FollowerState, timer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "follower") {
      event match {
        // If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
        case Timeout => startCandidate(state, timer)

        case RequestReceived(rv: RequestVote, respond) =>
          val (response, state2) = {
            // Reply false if term < currentTerm
            // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
            if ((rv.term < state.currentTerm) || state.votedForOtherThan(rv.candidateId) || state.hasEntriesAfter(rv.lastLog)) {
              (RequestVoteResponse(state.currentTerm, voteGranted = false), state)
            } else {
              (RequestVoteResponse(state.currentTerm, voteGranted = true), state.copy(votedFor = Some(rv.candidateId)))
            }
          }

          val restartedTimerIfGranted = if (response.voteGranted) timer.restartElection else ZIO.succeed(timer)
          restartedTimerIfGranted.flatMap(newTimer => doRespond(response, respond) *> follower(state2, followerState, newTimer))

        case RequestReceived(ae: AppendEntries, respond) =>
          // Reply false if term < currentTerm (§5.1)
          // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
          val (response, state2) = if (ae.term < state.currentTerm || !state.hasEntryAtTerm(ae.prevLog)) {
            (AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false), state)
          } else {
            // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
            // Append any new entries not already in the log
            (
              AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = true),
              state.appendEntries(ae.entries, ae.prevLog.map(_.index)).updateCommitIndex(ae.leaderCommit)
            )
          }

          // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
          ZIO.foreach(state2.indexesToApply)(i => stateMachine(state2.log(i))).flatMap { _ =>
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            val state4 = state2.updateLastAppliedToCommit()

            timer.restartElection
              .flatMap(newTimer =>
                doRespond(response, respond) *> follower(state4, followerState.copy(leaderId = Some(ae.leaderId)), newTimer)
              )
          }

        case RequestReceived(_: NewEntry, respond) => doRespond(RedirectToLeaderResponse(followerState.leaderId), respond)

        // ignore
        case RequestReceived(_: RequestVoteResponse, _)   => ZIO.unit
        case RequestReceived(_: AppendEntriesResponse, _) => ZIO.unit
      }
    }

  private def startCandidate(state: ServerState, timer: Timer): UIO[Unit] = ZIO.logAnnotate(StateLogAnnotation, "candidate-start") {
    // On conversion to candidate, start election: Increment currentTerm, Vote for self
    val state2 = state.incrementTerm(nodeId)
    // Reset election timer
    ZIO.log(s"Became candidate (${state2.currentTerm})") *> timer.restartElection.flatMap(newTimer =>
      // Send RequestVote RPCs to all other servers
      ZIO.foreachPar(otherNodes)(otherNodeId => doSend(otherNodeId, RequestVote(state2.currentTerm, nodeId, state2.lastEntryTerm))) *>
        candidate(state2, CandidateState(1), newTimer)
    )
  }

  private def candidate(state: ServerState, candidateState: CandidateState, timer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "candidate") {
      nextEvent(state, timer) {
        // If election timeout elapses: start new election
        case Timeout => startCandidate(state, timer)

        case RequestReceived(_: RequestVote, respond) =>
          doRespond(RequestVoteResponse(state.currentTerm, voteGranted = false), respond) *> candidate(state, candidateState, timer)

        case r @ RequestReceived(ae: AppendEntries, respond) =>
          // If AppendEntries RPC received from new leader: convert to follower
          if state.currentTerm == ae.term
          then handleFollower(r, state, FollowerState(Some(ae.leaderId)), timer)
          else
            doRespond(AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false), respond) *> candidate(
              state,
              candidateState,
              timer
            )

        case RequestReceived(_: NewEntry, respond) => doRespond(RedirectToLeaderResponse(None), respond)
        case RequestReceived(RequestVoteResponse(_, voteGranted), _) if voteGranted =>
          val candidateState2 = candidateState.modify(_.receivedVotes).using(_ + 1)
          // If votes received from majority of servers: become leader
          if candidateState2.receivedVotes > majority
          then startLeader(state, timer)
          else candidate(state, candidateState2, timer)
        case RequestReceived(_: RequestVoteResponse, _) => ZIO.unit

        // ignore
        case RequestReceived(_: AppendEntriesResponse, _) => ZIO.unit
      }
    }

  private def startLeader(state: ServerState, timer: Timer): UIO[Unit] = ZIO.logAnnotate(StateLogAnnotation, "leader-start") {
    val leaderState = LeaderState(
      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
      otherNodes.map(_ -> state.lastEntryTerm.map(_.index).fold(LogIndex(0))(i => LogIndex(i + 1))).toMap,
      // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
      otherNodes.map(_ -> None).toMap,
      Vector.empty
    )

    // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
    ZIO.log(s"Became leader (${state.currentTerm})") *> sendAppendEntries(state, leaderState, timer).flatMap(newTimer =>
      leader(state, leaderState, newTimer)
    )
  }

  private def leader(state: ServerState, leaderState: LeaderState, timer: Timer): UIO[Unit] =
    ZIO.logAnnotate(StateLogAnnotation, "leader") {
      nextEvent(state, timer) {
        // repeat during idle periods to prevent election timeouts (§5.2)
        case Timeout =>
          sendAppendEntries(state, leaderState, timer).flatMap(newTimer => leader(state, leaderState, newTimer))

        // If command received from client: append entry to local log
        case RequestReceived(NewEntry(value), respond) =>
          val state2 = state.appendEntry(LogEntry(value, state.currentTerm))
          val leaderState2 = leaderState.addAwaitingResponse(LogIndex(state.log.length - 1), doRespond(NewEntryAddedResponse, respond))
          sendAppendEntries(state2, leaderState2, timer).flatMap(leader(state2, leaderState2, _))

        // If successful: update nextIndex and matchIndex for follower (§5.3)
        case RequestReceived(AppendEntriesResponse(_, true, followerId, range), _) =>
          val leaderState2 = leaderState.appendSuccessful(followerId, range.map(_._2))
          // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
          val newCommitIndex = leaderState.commitIndex(if state.log.isEmpty then None else Some(LogIndex(state.log.length - 1)), majority)
          if state.commitIndex != newCommitIndex
          then
            val state2 = state.modify(_.commitIndex).setTo(newCommitIndex)
            val (leaderState3, responses) = newCommitIndex match
              case None     => (leaderState2, Vector.empty)
              case Some(ci) => leaderState2.removeAwaitingResponses(ci)

            // respond after entry applied to state machine (§5.3)
            ZIO.foreachPar(responses)(identity) *> sendAppendEntries(state2, leaderState3, timer).flatMap(leader(state2, leaderState3, _))
          else leader(state, leaderState2, timer)

        // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
        case RequestReceived(AppendEntriesResponse(_, false, followerId, range), _) =>
          val leaderState2 = leaderState.appendFailed(followerId, range.map(_._1))
          sendAppendEntry(followerId, state, leaderState2) *> leader(state, leaderState2, timer)

        // ignore
        case RequestReceived(_: RequestVote, _)         => ZIO.unit
        case RequestReceived(_: RequestVoteResponse, _) => ZIO.unit
        case RequestReceived(_: AppendEntries, _)       => ZIO.unit
      }
    }

  private def sendAppendEntries(state: ServerState, leaderState: LeaderState, timer: Timer): UIO[Timer] =
    timer.restartHeartbeat.flatMap(newTimer => ZIO.foreachPar(otherNodes)(sendAppendEntry(_, state, leaderState)).as(newTimer))

  private def sendAppendEntry(otherNodeId: NodeId, state: ServerState, leaderState: LeaderState): UIO[Unit] = {
    // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    val nextIndex = leaderState.nextIndex(otherNodeId)
    val prevEntry =
      if nextIndex == 0 then None
      else
        val prev = LogIndex(nextIndex - 1)
        Some(LogIndexTerm(state.log(prev).term, prev))

    doSend(otherNodeId, AppendEntries(state.currentTerm, nodeId, prevEntry, state.log.drop(nextIndex), state.commitIndex))
  }

  private def nextEvent(state: ServerState, timer: Timer)(next: ServerEvent => UIO[Unit]): UIO[Unit] =
    events.take.flatMap {
      // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
      case e @ RequestReceived(msg: FromServerMessage, _) if msg.term > state.currentTerm =>
        timer.restartElection.flatMap(newTimer => handleFollower(e, state.updateTerm(msg.term), FollowerState(None), newTimer))
      case e => next(e)
    }

  private def doSend(to: NodeId, msg: ToServerMessage): UIO[Unit] = ZIO.log(s"Send to ${to.id}: $msg") *> send(to, msg)
  private def doRespond(msg: ResponseMessage, respond: ResponseMessage => UIO[Unit]) = ZIO.log(s"Response: $msg") *> respond(msg)
}

object Saft extends ZIOAppDefault with Logging {
  def run: Task[Unit] = {
    val numberOfNodes = 5
    val electionTimeoutDuration = Duration.fromMillis(2000)
    val heartbeatTimeoutDuration = Duration.fromMillis(1000)
    val electionRandomization = 500

    val nodes = (1 to numberOfNodes).map(i => NodeId(s"node$i"))
    for {
      eventQueues <- ZIO.foreach(nodes)(nodeId => Queue.unbounded[ServerEvent].map(nodeId -> _)).map(_.toMap)
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
      heartbeatTimeout = ZIO.sleep(heartbeatTimeoutDuration)
      initial = nodes.map(nodeId =>
        new Node(nodeId, eventQueues(nodeId), send(nodeId), stateMachines(nodeId), nodes.toSet, electionTimeout, heartbeatTimeout)
      )
      fibers <- ZIO.foreach(initial)(_.start(ServerState(Vector.empty, None, Term(0), None, None)).fork)
      _ <- ZIO.log(s"$numberOfNodes nodes started. Press any key to exit.")
      _ <- Console.readLine
      _ <- ZIO.foreach(fibers)(f => f.interrupt *> f.join)
      _ <- ZIO.log("Bye!")
    } yield ()
  }
}
