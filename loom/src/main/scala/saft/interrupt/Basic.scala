package saft.interrupt

object Basic extends App {
  Thread.setDefaultUncaughtExceptionHandler((t, e) => {
    println(s"CAUGHT ${e.getClass} from $t")
  })

  // we can also use t.setUncaughtExceptionHandler()
  val t = Thread.startVirtualThread(() => {
    Thread.sleep(1000)
    println("DONE")
  })

  t.interrupt()
  Thread.sleep(2000)
}
