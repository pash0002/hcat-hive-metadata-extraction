package com.datametica.extraction.hive

import com.datametica.extraction.hive.json.JsonWriter
import com.datametica.extraction.hive.service.MetadataService
import com.datametica.extraction.hive.util.Conf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.logging.log4j.kotlin.Logging

class MetadataExtractor(conf: Conf, client: IMetaStoreClient): Logging {

  private val writer = JsonWriter(conf)
  private val metadataService = MetadataService(client)
  private val dbNames = metadataService.getAllDatabaseNames()

  fun extract() {
    logger.info("Extraction started.")
    extractAllDatabases()
    extractAllTablesAndPartitions()
    closeConnection()
  }

  fun extractAllDatabases() {
    logger.info("Extracting database metadata.")
    val databases = getAllDatabases()
    writer.serializeDatabases(databases)
    logger.info("Database metadata extracted.")
  }

  private fun getAllDatabases(): List<Database> {
    return dbNames.mapNotNull { metadataService.getDatabase(it) }
  }

  fun extractAllTablesAndPartitions() {
    logger.info("Extracting table metadata and partition stats.")
    dbNames.forEach { extractTablesAndPartitions(it) }
    logger.info("Table metadata and partition stats extracted.")
  }

  private fun extractTablesAndPartitions(dbName: String) {
    val tables = getTables(dbName)
    val tableNames = tables.filter { it.partitionKeys.size > 0 }.map { it.tableName }
    extractPartitionStats(dbName, tableNames)
    writer.serializeTables(dbName, tables)
  }

  fun extractPartitionStats(dbName: String, tableNames: List<String>) {
    val partitions = getPartitionStats(dbName, tableNames)
    writer.serializePartitionStats(dbName, partitions)
  }

  private fun getTables(dbName: String): List<Table> {
    val listOfTableNames = metadataService.getAllTableNames(dbName)
    return listOfTableNames.mapNotNull { tableName: String ->
      metadataService.getTable(dbName, tableName)
    }
  }

  private fun getPartitionStats(dbName: String, tableNames: List<String>): Map<String, Int> {
    return tableNames.map { tableName ->
      "${dbName}.${tableName}" to metadataService.getPartitions(dbName, tableName).count()
    }.toMap()
  }

  private fun closeConnection() {
    metadataService.close()
  }

}