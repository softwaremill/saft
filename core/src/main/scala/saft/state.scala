package saft

import zio.UIO

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

  def voteFor(other: NodeId): ServerState = copy(votedFor = Some(other))

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

object ServerState:
  val Initial: ServerState = ServerState(Vector.empty, None, Term(0), None, None)

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

case class FollowerState(leaderId: Option[NodeId]) {
  def leaderId(other: NodeId): FollowerState = copy(leaderId = Some(other))
}
case class CandidateState(receivedVotes: Int)
