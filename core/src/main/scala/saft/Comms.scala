package saft

import zio.UIO

/** Provides communication for a single server node. */
trait Comms:
  /** Next event from the queue. */
  def next: UIO[ServerEvent]

  /** Send an inter-server message. Might or might not wait for a response. */
  def send(toNodeId: NodeId, msg: RequestMessage with FromServerMessage): UIO[Unit]

  /** Add an event to own queue. */
  def add(event: ServerEvent): UIO[Unit]
