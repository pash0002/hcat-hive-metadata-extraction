package com.datametica.extraction.hive.service

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.Partition
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.logging.log4j.kotlin.Logging

class MetadataService(private val client: IMetaStoreClient): Logging {

  fun getAllDatabaseNames(): List<String> {
    try {
      return client.getDatabases("*")
    } catch (e: Exception) {
      logger.error("Exception while listing db names.")
      throw RuntimeException("Exception while listing db names.", e)
    }
  }

  fun getDatabase(dbName: String): Database? {
    try {
      logger.debug("Fetching metadata for database[$dbName]")
      return client.getDatabase(checkDB(dbName))
    } catch (e: Exception) {
      logger.error("Exception while fetching database[$dbName]: $e")
    }
    return null
  }

  fun getAllTableNames(dbName: String): List<String> {
    try {
      logger.debug("Fetching all table names in db[$dbName].")
      return client.getTables(checkDB(dbName), "*")
    } catch (e: Exception) {
      logger.error("Exception while fetching table names for db[$dbName]: $e")
    }
    return emptyList()
  }

  fun getTable(dbName: String, tableName: String): Table? {
    try {
      logger.debug("Fetching metadata for table[$dbName.$tableName]")
      return client.getTable(checkDB(dbName), tableName)
    } catch (e: Exception) {
      logger.error("Exception while fetching table[$dbName.$tableName]: $e")
    }
    return null
  }

  fun getPartitions(dbName: String, tableName: String): List<Partition> {
    try {
      logger.debug("Fetching partition metadata for table[$dbName.$tableName]")
      return client.listPartitions(checkDB(dbName), tableName, -1)
    } catch (e: Exception) {
      logger.error("Exception while fetching partitions for table[$dbName.$tableName]: $e")
    }
    return emptyList()
  }

  private fun checkDB(name: String): String {
    return if (StringUtils.isEmpty(name)) MetaStoreUtils.DEFAULT_DATABASE_NAME else name
  }

  fun close() {
    logger.info("Closing connection.")
    client.close()
  }

}
