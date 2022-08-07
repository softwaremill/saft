package saft

import zio.UIO

/** Provides communication for a single server node. */
trait Comms:
  def nextEvent: UIO[ServerEvent]
  def send(toNodeId: NodeId, msg: RequestMessage with ToServerMessage): UIO[Unit]
  def add(event: ServerEvent): UIO[Unit]
