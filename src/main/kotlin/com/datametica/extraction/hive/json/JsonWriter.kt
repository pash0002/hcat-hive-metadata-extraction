package com.datametica.extraction.hive.json

import com.datametica.extraction.hive.util.Conf
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.logging.log4j.kotlin.Logging
import java.io.File

class JsonWriter(private val conf: Conf) : Logging {

  private val objectMapper = ObjectMapper()
  private val separator = File.separator

  init {
    createOutputDirectory()
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    if (conf.beautifyJson != null && conf.beautifyJson) {
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT)
    }
  }

  fun serializeDatabases(listOfHCatDatabase: List<Database>?) {
    objectMapper.writeValue(File("${conf.outputDirectory}${separator}database.json"), listOfHCatDatabase)
    logger.debug("Database metadata serialized.")
  }

  fun serializeTables(dbName: String, listOfHCatTable: List<Table>) {
    objectMapper
        .writeValue(File("${conf.outputDirectory}${separator}table${separator}${dbName}_tables.json"), listOfHCatTable)
    logger.debug("Table metadata extracted serialized for database[$dbName]")
  }

  fun serializePartitionStats(dbName: String, listOfHCatPartition: Map<String, Int>) {
    objectMapper
        .writeValue(File("${conf.outputDirectory}${separator}partition${separator}${dbName}_partition.json"), listOfHCatPartition)
    logger.debug("Partition metadata extracted serialized.")
  }

  private fun createOutputDirectory() {
    val outputDir = File(conf.outputDirectory)

    if (outputDir.exists()) {
      val errorMsg = "Output dir ${conf.outputDirectory} already exists."
      logger.error(errorMsg)
      throw RuntimeException(errorMsg)
    }

    val canonicalPath = outputDir.canonicalPath
    File(canonicalPath + "${separator}table").mkdirs()
    File(canonicalPath + "${separator}partition").mkdirs()
    logger.debug("Output directory created.")
  }

}