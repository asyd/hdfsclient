package io.kensu

/**
  * Created by asyd on 21/01/17.
  */

case class Arguments(verbose: Boolean = false,
                     hdfsURL: String = "",
                     path: String = "/",
                     recursive: Boolean = false,
                     command: String = "ls"
                    )


object ArgParse {
  val parser = new scopt.OptionParser[Arguments]("scopt") {
    head("HDFS Client", "0.0.1")
    opt[Unit]('v', "verbose").action((_, c) => c.copy(verbose = true)).text("Increase verbosity")
    opt[String]("hdfsURL").action((x, c) => c.copy(hdfsURL = x)).text("HDFS URL")
    help("help").text("Display help")

    cmd("ls").action((_, c) => c.copy(command = "ls"))
      .text("Display files and directories")
      .children(
        opt[Unit]('R', "recurse").action((_, c) => c.copy(recursive = true)).text("Recurse")
      )
    cmd("find").action((_, c) => c.copy(command = "find"))
      .text("Display files and directories")
      .children(
        opt[Unit]('R', "recurse").action((_, c) => c.copy(recursive = true)).text("Recurse")
      )

  }

  def main(args: Array[String]): Unit = {
    val client = new HDFSClient()
    parser.parse(args, Arguments()) match {
      case Some(config) => {
        if (config.verbose)
          client.displaySettings
        config.command match {
          case "ls" => client.ls(config.path, config.recursive)
          case "find" => client.find()
        }
      }
      case None => println("KO")
    }
  }
}
