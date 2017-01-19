package io.kensu

import com.github.nscala_time.time.Imports._

/**
  * Created by asyd on 19/01/17.
  */
object Sandbox {
    def main(args: Array[String]): Unit = {
      val value:Long = 1484172182204L
      val mtime = new DateTime(value)
      println(mtime.toString("YYYY-MM-dd HH:mm"))
    }
}
