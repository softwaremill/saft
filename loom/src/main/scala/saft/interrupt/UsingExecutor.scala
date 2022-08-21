package saft.interrupt

import java.util.concurrent.Executors

object UsingExecutor extends App {
  val ec = Executors.newVirtualThreadPerTaskExecutor()
  val future = ec.submit((() => {
    //throw new RuntimeException("X")
    Thread.sleep(1000)
    println("DONE")
  }): Runnable)

  future.cancel(true)
  Thread.sleep(2000)
  println("future.isCancelled = " + future.isCancelled)
}
