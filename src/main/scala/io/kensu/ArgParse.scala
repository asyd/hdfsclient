package io.kensu

/**
  * Created by asyd on 21/01/17.
  */

case class Arguments(verbose: Boolean = false,
                     recursive: Boolean = false,
                     command: String = "ls",
                     paths: Seq[String] = Seq("/")
                    )


object ArgParse {
  val parser = new scopt.OptionParser[Arguments]("hdfscli") {
    head("HDFS Client", "0.0.1")
    opt[Unit]('v', "verbose").action((_, c) =>
      c.copy(verbose = true)).text("Increase verbosity")

    help("help").text("Display help")

    cmd("ls").action((_, c) => c.copy(command = "ls"))
      .text("Display files and directories")
      .children(
        opt[Unit]('R', "recurse").action((_, c) => c.copy(recursive = true)).text("Recurse"),
        arg[String]("<path>...").unbounded().optional().action((x, c) =>
          /* TODO: replace paths if provided */
          c.copy(paths = c.paths :+ x)).text("paths")
      )

    cmd("find").action((_, c) => c.copy(command = "find"))
      .text("Find files and directories and perform action")
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
          case "ls" => {
            config.paths.foreach(client.ls(_, config.recursive))
          }
          case "find" => client.find()
        }
      }
      case None => println("KO")
    }
  }
}
