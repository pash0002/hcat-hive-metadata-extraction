package com.datametica.extraction.hive.util

import org.apache.hadoop.hive.metastore.HiveMetaStore
import org.apache.logging.log4j.kotlin.Logging

class MetastoreRunner(private val conf: Conf): Runnable, Logging {

  override fun run() {
    try {
      val port = conf.thriftURL.split(":").last()
      logger.info("Starting metastore at port: $port")
      HiveMetaStore.main(arrayOf("-v", "-p", port))
    } catch (t: Throwable) {
      logger.error("Exiting. Got exception from metastore: ", t)
    }
  }
}