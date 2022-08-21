package saft.interrupt

import jdk.incubator.concurrent.StructuredTaskScope

object UsingScope extends App {
  val scope = new StructuredTaskScope[Any]()
  scope.fork(() => {
    Thread.sleep(1000)
    println("DONE")
  })
  scope.shutdown()
  scope.join()
  Thread.sleep(2000)
}
