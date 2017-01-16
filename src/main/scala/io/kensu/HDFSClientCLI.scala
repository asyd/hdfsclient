package io.kensu

import com.beust.jcommander.{JCommander, Parameter, ParameterException, Parameters}

/**
  * Created by asyd on 09/01/17.
  */

object HDFSClientCLI {
  def main(args: Array[String]) {
    //    var hdfsClient = new HDFSClient(args)
    val parameters = new CommonParameters
    // Don't ask me why, but new JCommander(parameters) doesn't work
    val jCommander = new JCommander()
    jCommander.addObject(parameters)

    val commandLs:CommandLs = new CommandLs
    jCommander.addCommand("ls", commandLs)

    val commandFind:CommandFind = new CommandFind
    jCommander.addCommand("find", commandFind)

    try {
      jCommander.parse(args: _*)

    } catch {
      case _: ParameterException => jCommander.usage()
    }
  }
}

class CommonParameters {
  @Parameter(names = Array("-v"), description = "Increase verbosity")
  var verbose: Boolean = false

  @Parameter(names = Array("--hdfsURL"), required = false)
  var hdfsURL: String = null
}

@Parameters(commandDescription = "List directory content")
class CommandLs {
  @Parameter(required = false)
  var path: String = "/"
}

@Parameters(commandDescription = "Search for files in a directory hierarchy")
class CommandFind {
  @Parameter(required = false)
  var path: String = "/"
}

//class HDFSClient(args: Array[String]) {
//  // See http://stackoverflow.com/questions/35779151/merging-multiple-typesafe-config-files-and-resolving-only-after-they-are-all-mer
//  val defaultConfig = ConfigFactory.parseResources("defaults.conf")
//  // TODO: Check if file exist, otherwise run the setup or display a link
//  val siteConfig = ConfigFactory.parseFile(new File(sys.env("HOME") + "/.config/hdfsclient.conf"))
//  val config = siteConfig.withFallback(defaultConfig).resolve()
//  val settings = new Settings(config)
//
//  println("===== HDFS Settings =====")
//  println("\thdfs.url       " + settings.hdfsURL)
//  println("\thdfs.security  " + settings.hdfsSecurity)
//
//  if (settings.hdfsSecurity == "kerberos") {
//    println("\t\thdfs.keytab    " + settings.hdfsKeytab)
//    println("\t\thdfs.principal " + settings.hdfsPrincipal)
//  } else {
//    println("\t\thdfs.user      " + settings.hdfsUser)
//    System.setProperty("HADOOP_USER_NAME", settings.hdfsUser)
//  }
//
//  val hdfsConfiguration = new Configuration()
//  hdfsConfiguration.set("fs.defaultFS", settings.hdfsURL)
//
//  val hdfs = FileSystem.get(hdfsConfiguration)
//  try {
//    def printEntry(entry: FileStatus): Unit = {
//      if (entry.isDirectory)
//        print("d")
//      else
//        print("-")
//      print(entry.getPermission + " ")
//      print(entry.getOwner + " ")
//      print(entry.getGroup + " ")
//      print(entry.getPath.toString.replaceFirst(s"^${settings.hdfsURL}/", ""))
//      println()
//    }
//
//    val path = new Path("/")
//    hdfs.listStatus(path).foreach((x: FileStatus) => printEntry(x))
//  } catch {
//    case e: Exception => println(e)
//  }
//
//}