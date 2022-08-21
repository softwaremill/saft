package saft

import com.typesafe.scalalogging.StrictLogging
import Logging.{setNodeLogAnnotation, setStateLogAnnotation}

import java.security.SecureRandom
import java.util.concurrent.{CompletableFuture, Future, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.duration.Duration

private enum NodeRole:
  def state: ServerState
  def timer: Timer

  case Follower(state: ServerState, followerState: FollowerState, timer: Timer) extends NodeRole
  case Candidate(state: ServerState, candidateState: CandidateState, timer: Timer) extends NodeRole
  case Leader(state: ServerState, leaderState: LeaderState, timer: Timer) extends NodeRole

/** A Raft node. Communicates with the outside world using [[comms]]. Committed logs are applied to [[stateMachine]]. */
class Node(nodeId: NodeId, comms: Comms, stateMachine: StateMachine, conf: Conf, persistence: Persistence) extends StrictLogging:
  def start(): Cancellable =
    Loom { loom =>
      setNodeLogAnnotation(nodeId)
      logger.info("Node started")
      try
        val initialTimer = Timer(loom, conf, comms)
        val timer = initialTimer.restartElection
        val initialState = persistence.get
        val role = NodeRole.Follower(initialState, FollowerState(None), timer)
        new NodeLoop(loom, nodeId, comms, stateMachine, conf, persistence).run(role)
      finally logger.info("Node stopped")
    }

private class NodeLoop(loom: Loom, nodeId: NodeId, comms: Comms, stateMachine: StateMachine, conf: Conf, persistence: Persistence)
    extends StrictLogging:
  private val otherNodes = conf.nodeIds.toSet - nodeId

  def run(role: NodeRole): Nothing = doRun(role)

  @tailrec
  private def doRun(role: NodeRole): Nothing =
    val event = comms.next

    role match
      case _: NodeRole.Follower  => setStateLogAnnotation("follower")
      case _: NodeRole.Candidate => setStateLogAnnotation("candidate")
      case _: NodeRole.Leader    => setStateLogAnnotation("leader")

    logger.debug(s"Next event: $event")

    val newRole = event match
      // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
      case e @ ServerEvent.RequestReceived(msg: FromServerMessage, _) if msg.term > role.state.currentTerm =>
        val state = role.state
        val newState = state.updateTerm(msg.term)

        val newTimer = role.timer.restartElection
        persistence(state, newState)
        handleEvent(e, NodeRole.Follower(newState, FollowerState(None), newTimer))
      case e => handleEvent(e, role)

    doRun(newRole)

  private def handleEvent(event: ServerEvent, role: NodeRole): NodeRole =
    val (response, newRole) = event match
      case ServerEvent.Timeout => (() => (), timeout(role))
      case ServerEvent.RequestReceived(rv: RequestVote, respond) =>
        val (r, nr) = requestVote(rv, role)
        (() => doRespond(r, respond), nr)
      case ServerEvent.RequestReceived(ae: AppendEntries, respond) =>
        val (r, nr) = appendEntries(ae, role)
        (() => doRespond(r, respond), nr)
      case ServerEvent.RequestReceived(ne: NewEntry, respond) =>
        println("NEW ENTRY " + ne)
        val (responseFuture, nr) = newEntry(ne, role)
        (() => { loom.fork { doRespond(responseFuture.get, respond) }; () }, nr)
      case ServerEvent.ResponseReceived(rvr: RequestVoteResponse)   => (() => (), requestVoteResponse(rvr, role))
      case ServerEvent.ResponseReceived(aer: AppendEntriesResponse) => (() => (), appendEntriesResponse(aer, role))

    persistAndRespond(response, role, newRole)

  private def timeout(role: NodeRole): NodeRole =
    role match
      // If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
      case NodeRole.Follower(state, _, timer) => startCandidate(state, timer)
      // If election timeout elapses: start new election
      case NodeRole.Candidate(state, _, timer) => startCandidate(state, timer)
      // repeat during idle periods to prevent election timeouts (§5.2)
      case NodeRole.Leader(state, leaderState, timer) =>
        val newTimer = sendAppendEntries(state, leaderState, timer)
        NodeRole.Leader(state, leaderState, newTimer)

  private def startCandidate(state: ServerState, timer: Timer): NodeRole =
    // On conversion to candidate, start election: Increment currentTerm, Vote for self
    val newState = state.incrementTerm(nodeId)
    logger.info(s"Became candidate (term: ${newState.currentTerm})")

    // Reset election timer
    val newTimer = timer.restartElection

    // Send RequestVote RPCs to all other servers
    for (otherNodeId <- otherNodes) doSend(otherNodeId, RequestVote(newState.currentTerm, nodeId, newState.lastIndexTerm))

    NodeRole.Candidate(newState, CandidateState(1), newTimer)

  private def requestVote(rv: RequestVote, role: NodeRole): (RequestVoteResponse, NodeRole) = role match
    case follower @ NodeRole.Follower(state, followerState, timer) =>
      // Reply false if term < currentTerm
      // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
      if ((rv.term < state.currentTerm) || state.votedForOtherThan(rv.candidateId) || state.hasEntriesAfter(rv.lastLog)) {
        (RequestVoteResponse(state.currentTerm, voteGranted = false), follower)
      } else {
        val newState = state.voteFor(rv.candidateId)
        val newTimer = timer.restartElection

        (RequestVoteResponse(newState.currentTerm, voteGranted = true), NodeRole.Follower(newState, followerState, newTimer))
      }
    case candidate: NodeRole.Candidate => (RequestVoteResponse(candidate.state.currentTerm, voteGranted = false), candidate)
    case leader: NodeRole.Leader       => (RequestVoteResponse(leader.state.currentTerm, voteGranted = false), leader)

  @tailrec
  private def appendEntries(ae: AppendEntries, role: NodeRole): (AppendEntriesResponse, NodeRole) = role match
    case NodeRole.Follower(state, followerState, timer) =>
      val newFollowerState = followerState.leaderId(ae.leaderId)

      // Reply false if term < currentTerm (§5.1)
      // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
      if (ae.term < state.currentTerm || !state.hasEntryAtTerm(ae.prevLog)) {
        val response = AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false)
        val newTimer = timer.restartElection
        (response, NodeRole.Follower(state, newFollowerState, newTimer))
      } else {
        // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        // Append any new entries not already in the log
        val newState = state.appendEntries(ae.entries, ae.prevLog.map(_.index)).updateCommitIndex(ae.leaderCommit)
        val response = AppendEntriesResponse.to(nodeId, ae)(newState.currentTerm, success = true)

        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        val newTimer = timer.restartElection
        val newState2 = applyEntries(newState)

        (response, NodeRole.Follower(newState2, newFollowerState, newTimer))
      }

    case candidate @ NodeRole.Candidate(state, _, timer) =>
      // If AppendEntries RPC received from new leader: convert to follower
      if state.currentTerm == ae.term
      then appendEntries(ae, NodeRole.Follower(state, FollowerState(Some(ae.leaderId)), timer))
      else (AppendEntriesResponse.to(nodeId, ae)(state.currentTerm, success = false), candidate)

    case leader: NodeRole.Leader => (AppendEntriesResponse.to(nodeId, ae)(leader.state.currentTerm, success = false), leader)

  private def newEntry(ne: NewEntry, role: NodeRole): (Future[NewEntryAddedResponse], NodeRole) =
    def redirectToLeader(leaderId: Option[NodeId]) =
      val f = new CompletableFuture[NewEntryAddedResponse]()
      f.complete(RedirectToLeaderResponse(leaderId))
      (f, role)

    role match
      case NodeRole.Follower(_, FollowerState(leaderId), _) => redirectToLeader(leaderId)
      case _: NodeRole.Candidate                            => redirectToLeader(None)
      // If command received from client: append entry to local log
      case NodeRole.Leader(state, leaderState, timer) =>
        val f = new CompletableFuture[NewEntryAddedResponse]()
        val newState = state.appendEntry(LogEntry(ne.data, state.currentTerm))
        val newLeaderState = leaderState.addAwaitingResponse(
          LogIndex(state.log.length - 1),
          () => f.complete(NewEntryAddedSuccessfullyResponse)
        )
        val newTimer = sendAppendEntries(newState, newLeaderState, timer)
        (f, NodeRole.Leader(newState, newLeaderState, newTimer))

  private def requestVoteResponse(rvr: RequestVoteResponse, role: NodeRole): NodeRole = role match
    case follower: NodeRole.Follower => follower // ignore
    case NodeRole.Candidate(state, candidateState, timer) if rvr.voteGranted =>
      val newCandidateState = candidateState.increaseReceivedVotes
      // If votes received from majority of servers: become leader
      if newCandidateState.receivedVotes >= conf.majority
      then startLeader(state, timer)
      else NodeRole.Candidate(state, newCandidateState, timer)
    case candidate: NodeRole.Candidate => candidate
    case leader: NodeRole.Leader       => leader

  private def startLeader(state: ServerState, timer: Timer): NodeRole = {
    val leaderState = LeaderState(
      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
      otherNodes.map(_ -> state.lastIndexTerm.map(_.index).fold(LogIndex(0))(i => LogIndex(i + 1))).toMap,
      // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
      otherNodes.map(_ -> None).toMap,
      Vector.empty
    )

    // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
    logger.info(s"Became leader (term: ${state.currentTerm})")
    val newTimer = sendAppendEntries(state, leaderState, timer)
    NodeRole.Leader(state, leaderState, newTimer)
  }

  def appendEntriesResponse(aer: AppendEntriesResponse, role: NodeRole): NodeRole = role match
    case follower: NodeRole.Follower   => follower
    case candidate: NodeRole.Candidate => candidate
    // If successful: update nextIndex and matchIndex for follower (§5.3)
    case NodeRole.Leader(state, leaderState, timer) if aer.success =>
      val newLeaderState = leaderState.appendSuccessful(aer.followerId, aer.prevLog, aer.entryCount)
      // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
      val newCommitIndex =
        newLeaderState.commitIndex(if state.log.isEmpty then None else Some(LogIndex(state.log.length - 1)), conf.majority)
      if newCommitIndex.exists(_ > state.commitIndex.getOrElse(-1)) && state.log.lastOption.map(_.term).contains(state.currentTerm)
      then
        val newState = state.setCommitIndex(newCommitIndex)
        val (newLeaderState2, responses) = newCommitIndex match
          case None     => (newLeaderState, Vector.empty)
          case Some(ci) => newLeaderState.removeAwaitingResponses(ci)

        // respond after entry applied to state machine (§5.3)
        val newState2 = applyEntries(newState)
        loom.fork { for (response <- responses) response() }
        val newTimer = sendAppendEntries(newState2, newLeaderState2, timer)
        NodeRole.Leader(newState2, newLeaderState2, newTimer)
      else NodeRole.Leader(state, newLeaderState, timer)

    // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
    case NodeRole.Leader(state, leaderState, timer) =>
      val newLeaderState = leaderState.appendFailed(aer.followerId, aer.prevLog)
      sendAppendEntry(aer.followerId, state, newLeaderState)
      NodeRole.Leader(state, newLeaderState, timer)

  //

  private def persistAndRespond(response: () => Unit, oldRole: NodeRole, newRole: NodeRole): NodeRole =
    if needsPersistence(oldRole, newRole) then persistence(oldRole.state, newRole.state)
    // (Updated on stable storage before responding to RPCs)
    response()
    newRole

  private def needsPersistence(oldRole: NodeRole, newRole: NodeRole): Boolean =
    oldRole.state.log != newRole.state.log || oldRole.state.votedFor != newRole.state.votedFor || oldRole.state.currentTerm != newRole.state.currentTerm

  private def sendAppendEntries(state: ServerState, leaderState: LeaderState, timer: Timer): Timer =
    val newTimer = timer.restartHeartbeat
    for (otherNode <- otherNodes) sendAppendEntry(otherNode, state, leaderState)
    newTimer

  private def sendAppendEntry(otherNodeId: NodeId, state: ServerState, leaderState: LeaderState): Unit =
    // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    val nextIndex = leaderState.nextIndex(otherNodeId)
    val prevEntry =
      if nextIndex == 0 then None
      else
        val prev = LogIndex(nextIndex - 1)
        Some(LogIndexTerm(state.log(prev).term, prev))

    doSend(otherNodeId, AppendEntries(state.currentTerm, nodeId, prevEntry, state.log.drop(nextIndex), state.commitIndex))

  private def applyEntries(state: ServerState): ServerState =
    for (i <- state.indexesToApply) stateMachine(state.log(i).data)
    // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    state.updateLastAppliedToCommit()

  private def doSend(to: NodeId, msg: RequestMessage with FromServerMessage): Unit =
    loom.fork {
      logger.debug(s"Send to node${to.number}: $msg")
      comms.send(to, msg)
    }

  private def doRespond(msg: ResponseMessage, respond: ResponseMessage => Unit): Unit =
    logger.debug(s"Response: $msg")
    respond(msg)

end NodeLoop

/** @param currentTimer
  *   The currently running timer - a fiber, which eventually adds a [[Timeout]] event using [[comms.add]]. That fiber can be interrupted to
  *   cancel the timer.
  */
private class Timer(loom: Loom, conf: Conf, comms: Comms, currentTimer: Cancellable):
  private def restart(timeout: Duration): Timer =
    currentTimer.cancel()
    val newTimer = loom.fork {
      Thread.sleep(timeout.toMillis)
      comms.add(ServerEvent.Timeout)
    }
    new Timer(loom, conf, comms, newTimer)

  def restartElection: Timer = restart(nextElectionTimeout)
  def restartHeartbeat: Timer = restart(conf.heartbeatTimeoutDuration)

  def nextElectionTimeout: Duration =
    conf.electionTimeoutDuration.plus(Duration(Timer.random.nextLong(conf.electionRandomization.toMillis), TimeUnit.MILLISECONDS))

private object Timer:
  private val random = new SecureRandom()

  def apply(loom: Loom, conf: Conf, comms: Comms): Timer =
    new Timer(loom, conf, comms, () => ())
