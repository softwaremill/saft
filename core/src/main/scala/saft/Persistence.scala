package saft

import zio.UIO

trait Persistence:
  def apply(oldState: ServerState, newState: ServerState): UIO[Unit]
  def get: UIO[ServerState]
