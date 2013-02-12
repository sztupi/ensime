package org.ensime.util {
object Profiling {
  def time[R](desc:String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$desc : elapsed time: " + (t1 - t0) / 1000000000.0 + "s")
    result
  }
}

}
