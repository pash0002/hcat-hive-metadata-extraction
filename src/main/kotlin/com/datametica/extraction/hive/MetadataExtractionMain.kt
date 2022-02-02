package com.datametica.extraction.hive

import com.datametica.extraction.hive.connection.MetastoreClientUtil
import com.datametica.extraction.hive.util.PropertiesFileUtil
import org.apache.logging.log4j.kotlin.Logging

class MetadataExtractionMain: Logging {

  fun begin(propertyFilePath: String) {
    val conf = PropertiesFileUtil.readPropertyFile(propertyFilePath)
    val client = MetastoreClientUtil.createClient(conf)
    val metadataExtractor = MetadataExtractor(conf, client)
    logger.info("Starting extraction.")
    metadataExtractor.extract()
    logger.info("Extraction completed.")
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>){
      if (args.isEmpty()) throw RuntimeException("Expected config.properties file path.")
      MetadataExtractionMain().begin(args[0])
    }
  }

}
