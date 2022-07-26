package saft

import zio.UIO

/** Provides communication for a single server node, managing a server's event queue and providing a way to add events to event queues of
  * other servers.
  */
trait Comms:
  /** Next event from the queue. */
  def next: UIO[ServerEvent]

  /** Send an inter-server message. Might or might not wait for a response. The response should eventually be added to the event queue, if
    * the request was successful.
    */
  def send(toNodeId: NodeId, msg: RequestMessage with FromServerMessage): UIO[Unit]

  /** Add an event to own queue. */
  def add(event: ServerEvent): UIO[Unit]
