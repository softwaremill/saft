package saft

import zio.{Exit, Fiber, Promise, UIO, ZIO}

import scala.annotation.tailrec

private enum NodeRole:
  def state: ServerState
  def timer: Timer

  case Follower(state: ServerState, followerState: FollowerState, timer: Timer) extends NodeRole
  case Candidate(state: ServerState, candidateState: CandidateState, timer: Timer) extends NodeRole
  case Leader(state: ServerState, leaderState: LeaderState, timer: Timer) extends NodeRole

/** A Raft node. Communicates with the outside world using [[comms]]. Committed logs are applied to [[stateMachine]]. */
class Node(nodeId: NodeId, comms: Comms, stateMachine: StateMachine, conf: Conf, persistence: Persistence) {
  private val otherNodes = conf.nodeIds.toSet - nodeId

  def start: UIO[Nothing] = (for {
    _ <- setNodeLogAnnotation(nodeId)
    _ <- ZIO.log("Node started")
    initialTimer <- Timer(conf, comms)
    timer <- initialTimer.restartElection
    initialState <- persistence.get
    role = NodeRole.Follower(initialState, FollowerState(None), timer)
    result <- run(role)
  } yield result).onExit(_ => ZIO.log("Node stopped"))

  private def run(role: NodeRole): UIO[Nothing] =
    comms.next
      .tap(_ =>
        role match
          case _: NodeRole.Follower  => setStateLogAnnotation("follower")
          case _: NodeRole.Candidate => setStateLogAnnotation("candidate")
          case _: NodeRole.Leader    => setStateLogAnnotation("leader")
      )
      .tap(e => ZIO.logDebug(s"Next event: $e"))
      .flatMap {
        // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
        case e @ RequestReceived(msg: FromServerMessage, _) if msg.term > role.state.currentTerm =>
          val state = role.state
          val newState = state.updateTerm(msg.term)
          for {
            newTimer <- role.timer.restartElection
            _ <- persistence(state, newState)
            newRole <- handleEvent(e, NodeRole.Follower(newState, FollowerState(None), newTimer))
          } yield newRole
        case e => handleEvent(e, role)
      }
      .flatMap(run)

  private def handleEvent(event: ServerEvent, role: NodeRole): UIO[NodeRole] =
    (event match
      case Timeout => timeout(role).map((ZIO.unit, _))
      case RequestReceived(rv: RequestVote, respond) =>
        requestVote(rv, role).map((response, newRole) => (doRespond(response, respond), newRole))
      case RequestReceived(ae: AppendEntries, respond) =>
        appendEntries(ae, role).map((response, newRole) => (doRespond(response, respond), newRole))
      case RequestReceived(ne: NewEntry, respond) =>
        newEntry(ne, role).map((responsePromise, newRole) => (responsePromise.await.flatMap(doRespond(_, respond)).fork.unit, newRole))
      case ResponseReceived(rvr: RequestVoteResponse)   => requestVoteResponse(rvr, role).map((ZIO.unit, _))
      case ResponseReceived(aer: AppendEntriesResponse) => appendEntriesResponse(aer, role).map((ZIO.unit, _))
    ).flatMap((response, newRole) => persistAndRespond(response, role, newRole))

  private def timeout(role: NodeRole): UIO[NodeRole] =
    role match
      // If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
      case NodeRole.Follower(state, _, timer) => startCandidate(state, timer)
      // If election timeout elapses: start new election
      case NodeRole.Candidate(state, _, timer) => startCandidate(state, timer)
      // repeat during idle periods to prevent election timeouts (§5.2)
      case NodeRole.Leader(state, leaderState, timer) =>
        sendAppendEntries(state, leaderState, timer).map(newTimer => NodeRole.Leader(state, leaderState, newTimer))

  private def startCandidate(state: ServerState, timer: Timer): UIO[NodeRole] =
    // On conversion to candidate, start election: Increment currentTerm, Vote for self
    val newState = state.incrementTerm(nodeId)
    // Reset election timer
    for {
      _ <- ZIO.log(s"Became candidate (term: ${newState.currentTerm})")
      newTimer <- timer.restartElection
      // Send RequestVote RPCs to all other servers
      _ <- ZIO.foreachDiscard(otherNodes)(otherNodeId =>
        doSend(otherNodeId, RequestVote(newState.currentTerm, nodeId, newState.lastEntryTerm))
      )
    } yield NodeRole.Candidate(newState, CandidateState(1), newTimer)

  private def requestVote(rv: RequestVote, role: NodeRole): UIO[(RequestVoteResponse, NodeRole)] = role match
    case follower @ NodeRole.Follower(state, followerState, timer) =>
      // Reply false if term < currentTerm
      // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
      if ((rv.term < state.currentTerm) || state.votedForOtherThan(rv.candidateId) || state.hasEntriesAfter(rv.lastLog)) {
        ZIO.succeed((RequestVoteResponse(state.currentTerm, voteGranted = false), follower))
      } else {
        val newState = state.voteFor(rv.candidateId)
        timer.restartElection.map(newTimer =>
          (RequestVoteResponse(newState.currentTerm, voteGranted = true), NodeRole.Follower(newState, followerState, newTimer))
        )
      }
    case candidate: NodeRole.Candidate => ZIO.succeed((RequestVoteResponse(candidate.state.currentTerm, voteGranted = false), candidate))
    case leader: NodeRole.Leader       => ZIO.succeed((RequestVoteResponse(leader.state.currentTerm, voteGranted = false), leader))

  @tailrec
  private def appendEntries(ae: AppendEntries, role: NodeRole): UIO[(AppendEntriesResponse, NodeRole)] = role match
    case NodeRole.Follower(state, followerState, timer) =>
      val newFollowerState = followerState.leaderId(ae.leaderId)

      // Reply false if term < currentTerm (§5.1)
      // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
      if (ae.term < state.currentTerm || !state.hasEntryAtTerm(ae.prevLog)) {
        val response = AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false)
        timer.restartElection.map(newTimer => (response, NodeRole.Follower(state, newFollowerState, newTimer)))
      } else {
        // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        // Append any new entries not already in the log
        val newState = state.appendEntries(ae.entries, ae.prevLog.map(_.index)).updateCommitIndex(ae.leaderCommit)
        val response = AppendEntriesResponse.to(nodeId, ae)(newState.currentTerm, success = true)

        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        for {
          newTimer <- timer.restartElection
          newState2 <- applyEntries(newState)
        } yield (response, NodeRole.Follower(newState2, newFollowerState, newTimer))
      }

    case candidate @ NodeRole.Candidate(state, _, timer) =>
      // If AppendEntries RPC received from new leader: convert to follower
      if state.currentTerm == ae.term
      then appendEntries(ae, NodeRole.Follower(state, FollowerState(Some(ae.leaderId)), timer))
      else ZIO.succeed((AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false), candidate))

    case leader: NodeRole.Leader => ZIO.succeed((AppendEntriesResponse.to(nodeId, ae)(leader.state.currentTerm, success = false), leader))

  private def newEntry(ne: NewEntry, role: NodeRole): UIO[(Promise[Nothing, NewEntryAddedResponse], NodeRole)] =
    def redirectToLeader(leaderId: Option[NodeId]) = for {
      p <- Promise.make[Nothing, NewEntryAddedResponse]
      _ <- p.succeed(RedirectToLeaderResponse(leaderId))
    } yield (p, role)

    role match
      case NodeRole.Follower(_, FollowerState(leaderId), _) => redirectToLeader(leaderId)
      case _: NodeRole.Candidate                            => redirectToLeader(None)
      // If command received from client: append entry to local log
      case NodeRole.Leader(state, leaderState, timer) =>
        for {
          p <- Promise.make[Nothing, NewEntryAddedResponse]
          newState = state.appendEntry(LogEntry(ne.data, state.currentTerm))
          newLeaderState = leaderState.addAwaitingResponse(
            LogIndex(state.log.length - 1),
            p.succeed(NewEntryAddedSuccessfullyResponse).unit
          )
          newTimer <- sendAppendEntries(newState, newLeaderState, timer)
        } yield (p, NodeRole.Leader(newState, newLeaderState, newTimer))

  private def requestVoteResponse(rvr: RequestVoteResponse, role: NodeRole): UIO[NodeRole] = role match
    case follower: NodeRole.Follower => ZIO.succeed(follower) // ignore
    case NodeRole.Candidate(state, candidateState, timer) if rvr.voteGranted =>
      val newCandidateState = candidateState.increaseReceivedVotes
      // If votes received from majority of servers: become leader
      if newCandidateState.receivedVotes >= conf.majority
      then startLeader(state, timer)
      else ZIO.succeed(NodeRole.Candidate(state, newCandidateState, timer))
    case candidate: NodeRole.Candidate => ZIO.succeed(candidate)
    case leader: NodeRole.Leader       => ZIO.succeed(leader)

  private def startLeader(state: ServerState, timer: Timer): UIO[NodeRole] = {
    val leaderState = LeaderState(
      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
      otherNodes.map(_ -> state.lastEntryTerm.map(_.index).fold(LogIndex(0))(i => LogIndex(i + 1))).toMap,
      // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
      otherNodes.map(_ -> None).toMap,
      Vector.empty
    )

    // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
    for {
      _ <- ZIO.log(s"Became leader (term: ${state.currentTerm})")
      newTimer <- sendAppendEntries(state, leaderState, timer)
    } yield NodeRole.Leader(state, leaderState, newTimer)
  }

  def appendEntriesResponse(aer: AppendEntriesResponse, role: NodeRole): UIO[NodeRole] = role match
    case follower: NodeRole.Follower   => ZIO.succeed(follower)
    case candidate: NodeRole.Candidate => ZIO.succeed(candidate)
    // If successful: update nextIndex and matchIndex for follower (§5.3)
    case NodeRole.Leader(state, leaderState, timer) if aer.success =>
      val newLeaderState = leaderState.appendSuccessful(aer.followerId, aer.prevLog, aer.entryCount)
      // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
      val newCommitIndex =
        newLeaderState.commitIndex(if state.log.isEmpty then None else Some(LogIndex(state.log.length - 1)), conf.majority)
      if newCommitIndex.exists(_ > state.commitIndex.getOrElse(-1)) && state.log.lastOption.map(_.term).contains(state.currentTerm)
      then
        val newState = state.commitIndex(newCommitIndex)
        val (newLeaderState2, responses) = newCommitIndex match
          case None     => (newLeaderState, Vector.empty)
          case Some(ci) => newLeaderState.removeAwaitingResponses(ci)

        // respond after entry applied to state machine (§5.3)
        for {
          newState2 <- applyEntries(newState)
          _ <- ZIO.foreachPar(responses)(identity)
          newTimer <- sendAppendEntries(newState2, newLeaderState2, timer)
        } yield NodeRole.Leader(newState2, newLeaderState2, newTimer)
      else ZIO.succeed(NodeRole.Leader(state, newLeaderState, timer))

    // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
    case NodeRole.Leader(state, leaderState, timer) =>
      val newLeaderState = leaderState.appendFailed(aer.followerId, aer.prevLog)
      sendAppendEntry(aer.followerId, state, newLeaderState).as(NodeRole.Leader(state, newLeaderState, timer))

  //

  private def persistAndRespond(response: UIO[Unit], oldRole: NodeRole, newRole: NodeRole): UIO[NodeRole] =
    val persist = if needsPersistence(oldRole, newRole) then persistence(oldRole.state, newRole.state) else ZIO.unit
    // (Updated on stable storage before responding to RPCs)
    for {
      _ <- persist
      _ <- response
    } yield newRole

  private def needsPersistence(oldRole: NodeRole, newRole: NodeRole): Boolean =
    oldRole.state.log != newRole.state.log || oldRole.state.votedFor != newRole.state.votedFor || oldRole.state.currentTerm != newRole.state.currentTerm

  private def sendAppendEntries(state: ServerState, leaderState: LeaderState, timer: Timer): UIO[Timer] =
    timer.restartHeartbeat.flatMap(newTimer => ZIO.foreach(otherNodes)(sendAppendEntry(_, state, leaderState)).as(newTimer))

  private def sendAppendEntry(otherNodeId: NodeId, state: ServerState, leaderState: LeaderState): UIO[Unit] =
    // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    val nextIndex = leaderState.nextIndex(otherNodeId)
    val prevEntry =
      if nextIndex == 0 then None
      else
        val prev = LogIndex(nextIndex - 1)
        Some(LogIndexTerm(state.log(prev).term, prev))

    doSend(otherNodeId, AppendEntries(state.currentTerm, nodeId, prevEntry, state.log.drop(nextIndex), state.commitIndex)).unit

  private def applyEntries(state: ServerState): UIO[ServerState] =
    ZIO.foreach(state.indexesToApply)(i => stateMachine(state.log(i).data)).as {
      // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      state.updateLastAppliedToCommit()
    }

  private def doSend(to: NodeId, msg: RequestMessage with FromServerMessage): UIO[Fiber.Runtime[Nothing, Unit]] =
    ZIO.logDebug(s"Send to node${to.number}: $msg") *> comms.send(to, msg).fork

  private def doRespond(msg: ResponseMessage, respond: ResponseMessage => UIO[Unit]) = ZIO.logDebug(s"Response: $msg") *> respond(msg)
}

/** @param currentTimer
  *   The currently running timer - a fiber, which eventually adds a [[Timeout]] event using [[comms.add]]. That fiber can be interrupted to
  *   cancel the timer.
  */
private class Timer(conf: Conf, comms: Comms, currentTimer: Fiber.Runtime[Nothing, Unit]):
  private def restart(timeout: UIO[Timeout.type]): UIO[Timer] =
    for {
      _ <- currentTimer.interrupt
      newFiber <- timeout.flatMap(comms.add).fork
    } yield new Timer(conf, comms, newFiber)
  def restartElection: UIO[Timer] = restart(conf.electionTimeout)
  def restartHeartbeat: UIO[Timer] = restart(conf.heartbeatTimeout)

private object Timer:
  def apply(conf: Conf, comms: Comms): UIO[Timer] = ZIO.never.fork.map(new Timer(conf, comms, _))
