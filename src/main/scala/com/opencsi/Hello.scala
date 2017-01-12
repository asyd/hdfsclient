package com.opencsi

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
  * Created by asyd on 09/01/17.
  */
object Hello {
  def main(args: Array[String]): Unit = {
    // See http://stackoverflow.com/questions/35779151/merging-multiple-typesafe-config-files-and-resolving-only-after-they-are-all-mer
    val defaultConfig = ConfigFactory.parseResources("defaults.conf")
    val siteConfig = ConfigFactory.load()
    val config = siteConfig.withFallback(defaultConfig).resolve()
    val settings = new Settings(config)

    println("===== HDFS Settings =====")
    println("\thdfs.url       " + settings.hdfsURL)
    println("\thdfs.security  " + settings.hdfsSecurity)

    if(settings.hdfsSecurity == "kerberos") {
      println("\t\thdfs.keytab    " + settings.hdfsKeytab)
      println("\t\thdfs.principal " + settings.hdfsPrincipal)
    } else {
      println("\t\thdfs.user      " + settings.hdfsUser)
      System.setProperty("HADOOP_USER_NAME", settings.hdfsUser)
    }

    val hdfsConfiguration = new Configuration()
    hdfsConfiguration.set("fs.defaultFS", settings.hdfsURL)

    val hdfs = FileSystem.get(hdfsConfiguration)
    try {
      def printEntry(entry: FileStatus): Unit = {
        if (entry.isDirectory)
          print("d")
        else
          print("-")
        print(entry.getPermission + " ")
        print(entry.getOwner + " ")
        print(entry.getGroup + " ")
        print(entry.getPath.toString.replaceFirst(s"^${settings.hdfsURL}/", ""))
        println()
      }
      val path = new Path("/")
      hdfs.listStatus(path).foreach((x: FileStatus) => printEntry(x))
    } catch {
      case e: Exception => println(e)
    }
  }

}
