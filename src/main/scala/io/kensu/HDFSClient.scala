package io.kensu

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}


/**
  * Created by asyd on 16/01/17.
  */

class HDFSClient(commonParameters: CommonParameters) {


  lazy val settings = {
    // See http://stackoverflow.com/questions/35779151/merging-multiple-typesafe-config-files-and-resolving-only-after-they-are-all-mer
    val defaultConfig = ConfigFactory.parseResources("defaults.conf")
    // TODO: Check if file exist, otherwise run the setup or display a link
    val siteConfig = ConfigFactory.parseFile(new File(sys.env("HOME") + "/.config/hdfsclient.conf"))
    val config = siteConfig.withFallback(defaultConfig).resolve()
    new Settings(config)
  }

  if (commonParameters.verbose) {

    println("===== HDFS Settings =====")
    println("\thdfs.url       " + settings.hdfsURL)
    println("\thdfs.security  " + settings.hdfsSecurity)

    if (settings.hdfsSecurity == "kerberos") {
      println("\t\thdfs.keytab    " + settings.hdfsKeytab)
      println("\t\thdfs.principal " + settings.hdfsPrincipal)
    } else {
      println("\t\thdfs.user      " + settings.hdfsUser)
      System.setProperty("HADOOP_USER_NAME", settings.hdfsUser)
    }
  }

  def printEntry(entry: FileStatus): Unit = {
    if (entry.isDirectory)
      print("d")
    else
      print("-")
    print(entry.getPermission + " ")
    print(entry.getOwner + " ")
    print(entry.getGroup + " ")
    print(entry.getPath.toString.replaceFirst(s"^${settings.hdfsURL}", ""))
    println()
  }

  def ls(path: String) {
    val hdfsConfiguration = new Configuration()
    hdfsConfiguration.set("fs.defaultFS", settings.hdfsURL)

    val hdfs = FileSystem.get(hdfsConfiguration)
    try {
      println(path + ":")
      val hdfsPath = new Path(path)
      hdfs.listStatus(hdfsPath).foreach((x: FileStatus) => printEntry(x))
    } catch {
      case e: Exception => println(e)
    }
  }
}