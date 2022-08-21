package saft

import java.util.concurrent.ArrayBlockingQueue

/** The state machine to which committed log entries are applied. */
trait StateMachine:
  def apply(entry: LogData): Unit

object StateMachine:
  /** Applies log data using the given [[doApply]] function in sequence, in the background, without blocking the caller (unless the queue
    * that buffers requests is full).
    */
  def background(loom: Loom, doApply: LogData => Unit): StateMachine =
    val toApplyQueue = new ArrayBlockingQueue[LogData](16)
    loom.fork {
      while (true) {
        doApply(toApplyQueue.take())
      }
    }

    (entry: LogData) => toApplyQueue.offer(entry)
