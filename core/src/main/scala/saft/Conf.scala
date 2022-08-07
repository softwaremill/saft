package saft

import zio.{Duration, UIO, ZIO}

case class Conf(numberOfNodes: Int, electionTimeoutDuration: Duration, heartbeatTimeoutDuration: Duration, electionRandomization: Duration):
  val nodeIds: Seq[NodeId] = (1 to numberOfNodes).map(NodeId.apply)
  val electionTimeout: UIO[Timeout.type] = ZIO.random
    .flatMap(_.nextLongBounded(electionRandomization.toMillis))
    .flatMap(randomization => ZIO.sleep(electionTimeoutDuration.plusMillis(randomization)))
    .as(Timeout)
  val heartbeatTimeout: UIO[Timeout.type] = ZIO.sleep(heartbeatTimeoutDuration).as(Timeout)

  val majority: Int = math.ceil(nodeIds.size.toDouble / 2).toInt

  def show: String = s"number of nodes = $numberOfNodes, " +
    s"election timeout = ${electionTimeoutDuration.toMillis}ms, " +
    s"heartbeat timeout = ${heartbeatTimeoutDuration.toMillis}ms, " +
    s"election randomization = ${electionRandomization.toMillis}ms"

object Conf:
  def default(numberOfNodes: Int): Conf = Conf(numberOfNodes, Duration.fromMillis(2000), Duration.fromMillis(500), Duration.fromMillis(500))
