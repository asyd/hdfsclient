package com.opencsi

import com.typesafe.config.Config

/**
  * Created by asyd on 09/01/17.
  */
class Settings(config: Config) {
  val hdfsUser = config.getString("hdfs.user")
  val hdfsURL = config.getString("hdfs.url")
  val hdfsSecurity = config.getString("hdfs.security").toLowerCase()
  val hdfsKeytab = config.getString("hdfs.keytab")
}
