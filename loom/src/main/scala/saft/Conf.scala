package saft

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

case class Conf(numberOfNodes: Int, electionTimeoutDuration: Duration, heartbeatTimeoutDuration: Duration, electionRandomization: Duration):
  val nodeIds: Seq[NodeId] = (1 to numberOfNodes).map(NodeId.apply)

  val majority: Int = math.ceil(nodeIds.size.toDouble / 2).toInt

  def show: String = s"number of nodes = $numberOfNodes, " +
    s"election timeout = ${electionTimeoutDuration.toMillis}ms, " +
    s"heartbeat timeout = ${heartbeatTimeoutDuration.toMillis}ms, " +
    s"election randomization = ${electionRandomization.toMillis}ms"

object Conf:
  def default(numberOfNodes: Int): Conf =
    Conf(numberOfNodes, Duration(2000, TimeUnit.MILLISECONDS), Duration(500, TimeUnit.MILLISECONDS), Duration(500, TimeUnit.MILLISECONDS))
