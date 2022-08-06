package saft

import zio.*

import java.io.IOException

/** A Raft node. Communicates with the outside world using [[events]] and [[send]]. Committed logs are applied to [[stateMachine]].
  * @param events
  *   The event queue for this node, with incoming events.
  * @param send
  *   Used for inter-node communication. Sends the given message to the given node.
  */
class Node(
    nodeId: NodeId,
    events: Queue[ServerEvent],
    send: (NodeId, ToServerMessage) => UIO[Unit],
    stateMachine: StateMachine,
    nodes: Set[NodeId],
    electionTimeout: UIO[Unit],
    heartbeatTimeout: UIO[Unit],
    persistence: Persistence
) {
  private val otherNodes = nodes - nodeId
  private val majority = math.ceil(nodes.size.toDouble / 2).toInt

  def start: UIO[Nothing] =
    setLogAnnotation(NodeIdLogAnnotation, nodeId.id) *>
      ZIO.log("Node started") *>
      Timer(electionTimeout, heartbeatTimeout, events)
        .flatMap(_.restartElection)
        .flatMap(timer => persistence.get.flatMap(state => follower(state, FollowerState(None), timer)))
        .onExit(_ => ZIO.log("Node stopped"))

  private def follower(state: ServerState, followerState: FollowerState, timer: Timer): UIO[Nothing] =
    setLogAnnotation(StateLogAnnotation, "follower") *> nextEvent(state, timer)(handleFollower(_, state, followerState, timer))

  private def handleFollower(event: ServerEvent, state: ServerState, followerState: FollowerState, timer: Timer): UIO[Nothing] =
    setLogAnnotation(StateLogAnnotation, "follower") *> {
      event match {
        // If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
        case Timeout => startCandidate(state, timer)

        case RequestReceived(rv: RequestVote, respond) =>
          // Reply false if term < currentTerm
          // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
          if ((rv.term < state.currentTerm) || state.votedForOtherThan(rv.candidateId) || state.hasEntriesAfter(rv.lastLog)) {
            doRespond(RequestVoteResponse(state.currentTerm, voteGranted = false), respond) *>
              follower(state, followerState, timer)
          } else {
            val state2 = state.voteFor(rv.candidateId)
            timer.restartElection.flatMap(timer2 =>
              persistence(state, state2) *>
                doRespond(RequestVoteResponse(state2.currentTerm, voteGranted = true), respond) *>
                follower(state2, followerState, timer2)
            )
          }

        case RequestReceived(ae: AppendEntries, respond) =>
          val followerState2 = followerState.leaderId(ae.leaderId)

          // Reply false if term < currentTerm (§5.1)
          // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
          if (ae.term < state.currentTerm || !state.hasEntryAtTerm(ae.prevLog)) {
            val response = AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false)
            timer.restartElection.flatMap(timer2 => doRespond(response, respond) *> follower(state, followerState2, timer2))
          } else {
            // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
            // Append any new entries not already in the log
            val state2 = state.appendEntries(ae.entries, ae.prevLog.map(_.index)).updateCommitIndex(ae.leaderCommit)
            val response = AppendEntriesResponse.to(nodeId, ae)(state2.currentTerm, success = true)

            // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
            timer.restartElection.flatMap { timer2 =>
              applyEntries(state2).flatMap { state3 =>
                persistence(state, state3) *> doRespond(response, respond) *> follower(state3, followerState2, timer2)
              }
            }
          }

        case RequestReceived(_: NewEntry, respond) =>
          doRespond(RedirectToLeaderResponse(followerState.leaderId), respond) *> follower(state, followerState, timer)

        // ignore
        case RequestReceived(_: RequestVoteResponse, _)   => follower(state, followerState, timer)
        case RequestReceived(_: AppendEntriesResponse, _) => follower(state, followerState, timer)
      }
    }

  private def startCandidate(state: ServerState, timer: Timer): UIO[Nothing] = setLogAnnotation(StateLogAnnotation, "candidate-start") *> {
    // On conversion to candidate, start election: Increment currentTerm, Vote for self
    val state2 = state.incrementTerm(nodeId)
    // Reset election timer
    ZIO.log(s"Became candidate (${state2.currentTerm})") *> timer.restartElection.flatMap(timer2 =>
      persistence(state, state2) *>
        // Send RequestVote RPCs to all other servers
        ZIO.foreachPar(otherNodes)(otherNodeId => doSend(otherNodeId, RequestVote(state2.currentTerm, nodeId, state2.lastEntryTerm))) *>
        candidate(state2, CandidateState(1), timer2)
    )
  }

  private def candidate(state: ServerState, candidateState: CandidateState, timer: Timer): UIO[Nothing] =
    setLogAnnotation(StateLogAnnotation, "candidate") *> nextEvent(state, timer) {
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

      case RequestReceived(_: NewEntry, respond) =>
        doRespond(RedirectToLeaderResponse(None), respond) *> candidate(state, candidateState, timer)

      case RequestReceived(RequestVoteResponse(_, voteGranted), _) if voteGranted =>
        val candidateState2 = candidateState.increaseReceivedVotes
        // If votes received from majority of servers: become leader
        if candidateState2.receivedVotes >= majority
        then startLeader(state, timer)
        else candidate(state, candidateState2, timer)
      case RequestReceived(_: RequestVoteResponse, _) => candidate(state, candidateState, timer)

      // ignore
      case RequestReceived(_: AppendEntriesResponse, _) => candidate(state, candidateState, timer)
    }

  private def startLeader(state: ServerState, timer: Timer): UIO[Nothing] = setLogAnnotation(StateLogAnnotation, "leader-start") *> {
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

  private def leader(state: ServerState, leaderState: LeaderState, timer: Timer): UIO[Nothing] =
    setLogAnnotation(StateLogAnnotation, "leader") *> nextEvent(state, timer) {
      // repeat during idle periods to prevent election timeouts (§5.2)
      case Timeout =>
        sendAppendEntries(state, leaderState, timer).flatMap(newTimer => leader(state, leaderState, newTimer))

      // If command received from client: append entry to local log
      case RequestReceived(NewEntry(value), respond) =>
        val state2 = state.appendEntry(LogEntry(value, state.currentTerm))
        val leaderState2 = leaderState.addAwaitingResponse(LogIndex(state.log.length - 1), doRespond(NewEntryAddedResponse, respond))
        persistence(state, state2) *> sendAppendEntries(state2, leaderState2, timer).flatMap(leader(state2, leaderState2, _))

      // If successful: update nextIndex and matchIndex for follower (§5.3)
      case RequestReceived(AppendEntriesResponse(_, true, followerId, range), _) =>
        val leaderState2 = leaderState.appendSuccessful(followerId, range.map(_._2))
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
        val newCommitIndex = leaderState.commitIndex(if state.log.isEmpty then None else Some(LogIndex(state.log.length - 1)), majority)
        if state.commitIndex != newCommitIndex
        then
          val state2 = state.commitIndex(newCommitIndex)
          val (leaderState3, responses) = newCommitIndex match
            case None     => (leaderState2, Vector.empty)
            case Some(ci) => leaderState2.removeAwaitingResponses(ci)

          // respond after entry applied to state machine (§5.3)
          applyEntries(state2).flatMap { state3 =>
            ZIO.foreachPar(responses)(identity) *> sendAppendEntries(state3, leaderState3, timer).flatMap(leader(state3, leaderState3, _))
          }
        else leader(state, leaderState2, timer)

      // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
      case RequestReceived(AppendEntriesResponse(_, false, followerId, range), _) =>
        val leaderState2 = leaderState.appendFailed(followerId, range.map(_._1))
        sendAppendEntry(followerId, state, leaderState2) *> leader(state, leaderState2, timer)

      // ignore
      case RequestReceived(_: RequestVote, _)         => leader(state, leaderState, timer)
      case RequestReceived(_: RequestVoteResponse, _) => leader(state, leaderState, timer)
      case RequestReceived(_: AppendEntries, _)       => leader(state, leaderState, timer)
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

  private def applyEntries(state: ServerState): UIO[ServerState] =
    ZIO.foreach(state.indexesToApply)(i => stateMachine(state.log(i).data)).as {
      // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      state.updateLastAppliedToCommit()
    }

  private def nextEvent(state: ServerState, timer: Timer)(next: ServerEvent => UIO[Nothing]): UIO[Nothing] =
    events.take.flatMap {
      // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
      case e @ RequestReceived(msg: FromServerMessage, _) if msg.term > state.currentTerm =>
        val state2 = state.updateTerm(msg.term)
        timer.restartElection.flatMap(timer2 => persistence(state, state2) *> handleFollower(e, state2, FollowerState(None), timer2))
      case e => next(e)
    }

  private def doSend(to: NodeId, msg: ToServerMessage): UIO[Unit] = ZIO.logDebug(s"Send to ${to.id}: $msg") *> send(to, msg)
  private def doRespond(msg: ResponseMessage, respond: ResponseMessage => UIO[Unit]) = ZIO.logDebug(s"Response: $msg") *> respond(msg)
}

/** @param electionTimeout
  *   The timeout for an election - used when starting an election timer using [[restartElection]].
  * @param heartbeatTimeout
  *   The timeout for a leader heartbeat (sending [[AppendEntries]]) - used when starting a heartbeat timer using [[restartHeartbeat]].
  * @param currentTimer
  *   The currently running timer - a fiber, which eventually puts a [[Timeout]] message to [[queue]]. Can be interrupted to cancel the
  *   timer.
  */
private class Timer(
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

private object Timer {
  def apply(electionTimeout: UIO[Unit], heartbeatTimeout: UIO[Unit], queue: Enqueue[Timeout.type]): UIO[Timer] =
    ZIO.never.fork.map(new Timer(electionTimeout, heartbeatTimeout, queue, _))
}
