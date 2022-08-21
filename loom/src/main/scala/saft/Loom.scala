package saft

import jdk.incubator.concurrent.StructuredTaskScope

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference

class Loom private (scope: StructuredTaskScope[Any]):
  def fork(t: => Unit): Cancellable =
    val future = scope.fork(() => t)
    () => future.cancel(true)

object Loom:
  def apply(t: Loom => Unit): Cancellable =
    val th = Thread.startVirtualThread { () =>
      val scope = new StructuredTaskScope[Any]()
      try t(new Loom(scope))
      catch case _: InterruptedException => () // ignore
      finally
        scope.join()
        scope.close()
    }
    () => th.interrupt()

trait Cancellable:
  def cancel(): Unit

object Cancellable:
  val Empty: Cancellable = () => ()
