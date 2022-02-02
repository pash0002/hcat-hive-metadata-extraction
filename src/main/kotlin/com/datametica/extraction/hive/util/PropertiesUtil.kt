package com.datametica.extraction.hive.util

import org.apache.logging.log4j.kotlin.Logging
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.util.*

object PropertiesFileUtil : Logging {

  private fun getPropertiesFromFile(propertyFile: String): Conf {
    val inputStream = FileInputStream(File(propertyFile))
    val properties = Properties()
    properties.load(inputStream)
    return Conf(properties)
  }

  fun readPropertyFile(propertyFilePath: String): Conf {
    try {
      logger.info("Loading properties file.")
      return getPropertiesFromFile(propertyFilePath)
    } catch (e: IOException) {
      logger.error("Error while loading properties file:", e)
      throw e
    }
  }

}