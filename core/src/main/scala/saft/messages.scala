package saft

sealed trait Message

/** A message (request or response) that can be sent to a server. */
sealed trait ToServerMessage extends Message

/** A message (request or response) that can be sent from a server. */
sealed trait FromServerMessage extends Message:
  def term: Term

/** A message that is a request; always to a server. */
sealed trait RequestMessage extends ToServerMessage

/** A message that is a response, either to a client or a server. */
sealed trait ResponseMessage extends Message

/** A message that can be sent to a client; always a response. */
sealed trait ToClientMessage extends ResponseMessage

case class RequestVote(term: Term, candidateId: NodeId, lastLog: Option[LogIndexTerm]) extends RequestMessage with FromServerMessage
case class RequestVoteResponse(term: Term, voteGranted: Boolean) extends ResponseMessage with ToServerMessage with FromServerMessage

case class AppendEntries(
    term: Term,
    leaderId: NodeId,
    prevLog: Option[LogIndexTerm],
    entries: Vector[LogEntry],
    leaderCommit: Option[LogIndex]
) extends RequestMessage
    with FromServerMessage

/** The extra [[followerId]], [[prevLog]] and [[entryCount]] are needed to correlate the response & request, so that the leader can update
  * [[LeaderState.nextIndex]] and [[LeaderState.matchIndex]] appropriately. Given an [[AppendEntries]] request message, a response can be
  * created using [[AppendEntriesResponse.to]].
  * @param followerId
  *   The id of the node which responds to the append entries request.
  * @param entryCount
  *   The number of entries sent using [[AppendEntries]].
  */
case class AppendEntriesResponse(term: Term, success: Boolean, followerId: NodeId, prevLog: Option[LogIndexTerm], entryCount: Int)
    extends ResponseMessage
    with ToServerMessage
    with FromServerMessage
object AppendEntriesResponse:
  def to(followerId: NodeId, appendEntries: AppendEntries)(term: Term, success: Boolean): AppendEntriesResponse =
    AppendEntriesResponse(term, success, followerId, appendEntries.prevLog, appendEntries.entries.length)

case class NewEntry(data: LogData) extends RequestMessage
case object NewEntryAddedResponse extends ToClientMessage

/** Response sent in case a [[NewEntry]] is received by a non-leader node. */
case class RedirectToLeaderResponse(leaderId: Option[NodeId]) extends ToClientMessage
