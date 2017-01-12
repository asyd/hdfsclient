package com.opencsi

import com.typesafe.config.Config

/**
  * Created by asyd on 09/01/17.
  */
class Settings(config: Config) {
  val hdfsSecurity = config.getString("hdfs.security").toLowerCase()
  val hdfsURL = config.getString("hdfs.url")

  /* Some parameters are dependent of others */
  val hdfsUser = if(config.hasPath("hdfs.user"))
    config.getString("hdfs.user") else null

  val hdfsKeytab = if(config.hasPath("hdfs.keytab"))
    config.getString("hdfs.keytab") else null
  val hdfsPrincipal = if(config.hasPath("hdfs.principal"))
    config.getString("hdfs.principal") else null
}
