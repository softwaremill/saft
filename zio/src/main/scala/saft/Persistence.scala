package saft

import zio.UIO

/** Provides persistence for the persistence part of [[ServerState]]: `currentTerm`, `votedFor` and `log`. */
trait Persistence:
  /** Persists the state. The [[oldState]] can be used to compute a delta of changes that need to be saved. */
  def apply(oldState: ServerState, newState: ServerState): UIO[Unit]
  def get: UIO[ServerState]
