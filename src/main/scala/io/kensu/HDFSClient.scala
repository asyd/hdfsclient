package io.kensu

import java.io.File

import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable


/**
  * Created by asyd on 16/01/17.
  */

class HDFSClient(commonParameters: CommonParameters) {
  val settings = {
    // See http://stackoverflow.com/questions/35779151/merging-multiple-typesafe-config-files-and-resolving-only-after-they-are-all-mer
    val defaultConfig = ConfigFactory.parseResources("defaults.conf")
    // TODO: Check if file exist, otherwise run the setup or display a link
    val siteConfig = ConfigFactory.parseFile(new File(sys.env("HOME") + "/.config/hdfsclient.conf"))
    val config = siteConfig.withFallback(defaultConfig).resolve()
    new Settings(config)
  }

  if (commonParameters.verbose) {

    println("===== HDFS Settings =====")
    println("hdfs.url       " + settings.hdfsURL)
    println("hdfs.security  " + settings.hdfsSecurity)

    if (settings.hdfsSecurity == "kerberos") {
      println("hdfs.keytab    " + settings.hdfsKeytab)
      println("hdfs.principal " + settings.hdfsPrincipal)
    } else {
      println("hdfs.user      " + settings.hdfsUser)
      System.setProperty("HADOOP_USER_NAME", settings.hdfsUser)
    }
  }


  def printEntry(entry: FileStatus) = {
    val path = entry.getPath
    def printDate(date: Long): String = {
      new DateTime(date).toString("YYYY-MM-dd HH:mm")
    }

    if (entry.isDirectory)
      print("d")
    else
      print("-")
    print(entry.getPermission + " ")
    print(entry.getOwner + " ")
    print(entry.getGroup + " ")
    print(printDate(entry.getModificationTime) + " ")
    print(entry.getPath.toString.replaceFirst(s"^${settings.hdfsURL}", ""))
    println()
  }

  def find(args: CommandFind): Unit = {

  }

  def readdir(hdfs: FileSystem, path: String, recurse: Boolean, callback: (FileStatus) => Unit) {
    val dirs = new mutable.Queue[FileStatus]
    val files = new mutable.Queue[FileStatus]
    if (recurse)
      println(s"${path}:")
    try {
      val hdfsPath = new Path(path)
      for (entry: FileStatus <- hdfs.listStatus(hdfsPath)) {
        if (entry.isDirectory) {
          dirs += entry
        } else {
          files += entry
        }
      }
      dirs.foreach((entry:FileStatus) =>
        callback(entry)
      )
      files.foreach((entry:FileStatus) =>
        callback(entry)
      )
    } catch {
      case e: Exception => println(e)
    }
  }

  def ls(args: CommandLs) {
    val hdfsConfiguration = new Configuration()
    hdfsConfiguration.set("fs.defaultFS", settings.hdfsURL)
    val hdfs = FileSystem.get(hdfsConfiguration)

    readdir(hdfs, args.path, args.recursive, printEntry)
  }
}