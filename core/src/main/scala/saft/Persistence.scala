package saft

import zio.{Ref, UIO, ZIO}

trait Persistence:
  def apply(oldState: ServerState, newState: ServerState): UIO[Unit]
  def get: UIO[ServerState]

class InMemoryPersistence(refs: Map[NodeId, Ref[ServerState]]) {
  def forNodeId(nodeId: NodeId): Persistence = new Persistence {
    private val ref = refs(nodeId)
    override def apply(oldState: ServerState, newState: ServerState): UIO[Unit] =
      ref.set(newState.copy(commitIndex = None, lastApplied = None))
    override def get: UIO[ServerState] = ref.get
  }
}

object InMemoryPersistence {
  def apply(nodeIds: Seq[NodeId]): UIO[InMemoryPersistence] =
    ZIO.foreach(nodeIds.toList)(nodeId => Ref.make(ServerState.Initial).map(nodeId -> _)).map(_.toMap).map(new InMemoryPersistence(_))
}
