package saft

import zio.{Enqueue, Queue, UIO}

/** The state machine to which committed log entries are applied. */
trait StateMachine:
  def apply(entry: LogData): UIO[Unit]

object StateMachine:
  /** Applies log data using the given [[doApply]] function in sequence, in the background, without blocking the caller (unless the queue
    * that buffers requests is full).
    */
  def background(doApply: LogData => UIO[Unit]): UIO[StateMachine] =
    for {
      toApplyQueue <- Queue.bounded[LogData](16)
      _ <- toApplyQueue.take.flatMap(doApply).forever.fork
    } yield new StateMachine:
      def apply(entry: LogData): UIO[Unit] = toApplyQueue.offer(entry).unit
