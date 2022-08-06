package saft

case class NodeId(id: String)

opaque type Term <: Int = Int
object Term:
  def apply(t: Int): Term = t

opaque type LogIndex <: Int = Int
object LogIndex:
  def apply(i: Int): LogIndex = i

case class LogEntry(value: String, term: Term)
case class LogIndexTerm(term: Term, index: LogIndex)
