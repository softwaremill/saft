package saft

case class NodeId(number: Int)

opaque type Term <: Int = Int
object Term:
  def apply(t: Int): Term = t

opaque type LogIndex <: Int = Int
object LogIndex:
  def apply(i: Int): LogIndex = i

opaque type LogData <: String = String
object LogData:
  def apply(d: String): LogData = d

case class LogEntry(data: LogData, term: Term)
case class LogIndexTerm(term: Term, index: LogIndex)
