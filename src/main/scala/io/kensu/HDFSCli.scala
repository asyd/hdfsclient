package io.kensu

import com.beust.jcommander.{JCommander, Parameter, ParameterException, Parameters}

/**
  * Created by asyd on 09/01/17.
  */

object HDFSCli {
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
      jCommander.parseWithoutValidation(args: _*)
      val hdfsClient = new HDFSClient(parameters)
      jCommander.getParsedCommand match {
        case "ls" => hdfsClient.ls(commandLs)
        case "find" => hdfsClient.find(commandFind)
      }
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
  @Parameter(names = Array("--path"), required = false)
  var path: String = "/"

  @Parameter(names = Array("-R"), required = false)
  var recursive: Boolean = false
}

@Parameters(commandDescription = "Search for files in a directory hierarchy")
class CommandFind {
  @Parameter(required = true)
  var path: String = "/"
}

