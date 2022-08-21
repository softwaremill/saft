package saft

import jdk.incubator.concurrent.StructuredTaskScope

class Loom():
  private val scope: StructuredTaskScope[Any] = new StructuredTaskScope[Any]()
  def fork(t: => Unit): Cancellable =
    val future = scope.fork(() => t)
    new Cancellable:
      override def cancel(): Unit = future.cancel(true)

  def close(): Unit = scope.close()

object Loom:
  def apply(t: Loom => Unit): Unit =
    val l = new Loom()
    try t(l)
    finally l.close()

trait Cancellable:
  def cancel(): Unit
