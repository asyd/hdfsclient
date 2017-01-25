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

class HDFSClient() {
  val settings = {
    // See http://stackoverflow.com/questions/35779151/merging-multiple-typesafe-config-files-and-resolving-only-after-they-are-all-mer
    val defaultConfig = ConfigFactory.parseResources("defaults.conf")
    // TODO: Check if file exist, otherwise run the setup or display a link
    val siteConfig = ConfigFactory.parseFile(new File(sys.env("HOME") + "/.config/hdfsclient.conf"))
    val config = siteConfig.withFallback(defaultConfig).resolve()
    new Settings(config)
  }

  val hdfs = {
    val hdfsConfiguration = new Configuration()
    hdfsConfiguration.set("fs.defaultFS", settings.hdfsURL)
    FileSystem.get(hdfsConfiguration)
  }

  def displaySettings: Unit = {
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
    println()
  }


  def formatEntryPath(entry: FileStatus): String = {
    return entry.getPath.toString.replaceFirst(s"^${settings.hdfsURL}", "")
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
    print(formatEntryPath(entry) + " ")
    println()
  }

  def readdir(hdfs: FileSystem, path: String, recurse: Boolean, callback: (FileStatus) => Unit) {
    val dirs = new mutable.Queue[FileStatus]
    val files = new mutable.Queue[FileStatus]

    try {
      val hdfsPath = new Path(path)
      for (entry: FileStatus <- hdfs.listStatus(hdfsPath)) {
        if (entry.isDirectory) {
          dirs += entry
        } else {
          files += entry
        }
      }
      /* Display directories first */
      dirs.foreach((entry: FileStatus) =>
        callback(entry)
      )
      /* Then files */
      files.foreach((entry: FileStatus) =>
        callback(entry)
      )
      if (recurse) {
        println()
        dirs.foreach((entry: FileStatus) =>
          readdir(hdfs, formatEntryPath(entry), recurse, callback)
        )
      }
    } catch {
      case e: Exception => println(e)
    }
  }

  def find(): Unit = {
    println("yeah")
  }

  def ls(path: String, recursive: Boolean) {
    println(s"${path}:")
    readdir(hdfs, path, recursive, printEntry)
  }
}