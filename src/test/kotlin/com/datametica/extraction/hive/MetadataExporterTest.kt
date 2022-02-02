package com.datametica.extraction.hive

import com.datametica.extraction.hive.connection.MetastoreClientUtil
import com.datametica.extraction.hive.serde.DatabaseDeserializer
import com.datametica.extraction.hive.serde.TableDeserializer
import com.datametica.extraction.hive.util.Conf
import com.datametica.extraction.hive.util.MetastoreRunner
import com.datametica.extraction.hive.util.NoExitSecurityManager
import com.datametica.extraction.hive.util.PropertiesFileUtil
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hive.hcatalog.api.SampleDatabase
import org.apache.logging.log4j.kotlin.Logging
import org.junit.AfterClass
import org.junit.BeforeClass
import java.io.File
import kotlin.test.Test

class MetadataExporterTest {

  companion object : Logging {
    private var securityManager: SecurityManager? = null
    private lateinit var client: IMetaStoreClient
    private lateinit var conf: Conf
    private lateinit var metadataExtractor: MetadataExtractor
    lateinit var sampleDatabase: SampleDatabase

    @BeforeClass
    @JvmStatic
    fun setup() {
      conf = PropertiesFileUtil.readPropertyFile("src/test/resources/config.properties")
      startMetastore()
      setSystemProperties()
      client = MetastoreClientUtil.createClient(conf)
      sampleDatabase = SampleDatabase(client).create()
      metadataExtractor = MetadataExtractor(conf, client)
    }

    private fun setSystemProperties() {
      securityManager = System.getSecurityManager()
      System.setSecurityManager(NoExitSecurityManager())
      System.setProperty(ConfVars.PREEXECHOOKS.varname, " ")
      System.setProperty(ConfVars.POSTEXECHOOKS.varname, " ")
    }

    private fun startMetastore() {
      Thread(MetastoreRunner(conf)).start()
      Thread.sleep(40000)
    }

    @AfterClass
    @JvmStatic
    @Throws(Exception::class)
    fun tearDown() {
      logger.info("Shutting down metastore.")
      System.setSecurityManager(securityManager)
      logger.info("Cleaning up files.")
      File(conf.outputDirectory).deleteRecursively()
      File("metastore_db").deleteRecursively()
    }

  }

  private val objectMapper =
      ObjectMapper()
          .registerModule(SimpleModule()
              .addDeserializer(Table::class.java, TableDeserializer())
              .addDeserializer(Database::class.java, DatabaseDeserializer()))

  @Test
  fun `Database serialization`() {
    metadataExtractor.extractAllDatabases()
    val actualFile = File("${conf.outputDirectory}/database.json")
    val databases = objectMapper.readValue(actualFile, Array<Database>::class.java)
    assert(databases.map { it.name }.contains(SampleDatabase.DB))
    assert(databases.map { it.locationUri }.contains("file:/user/hive/warehouse/testdb.db"))
  }

  @Test
  fun `Table serialization`() {
    metadataExtractor.extractAllTablesAndPartitions()
    val actualFile = File("${conf.outputDirectory}/table/testdb_tables.json")
    val tables = objectMapper.readValue(actualFile, Array<Table>::class.java)
    val tableNames = tables.map { it.tableName }

    assert(tableNames.contains(SampleDatabase.SIMPLE_TABLE))
    val simpleTable = tables.find { it.tableName == SampleDatabase.SIMPLE_TABLE }!!
    assert(compareCols(simpleTable, sampleDatabase.simpleTable))

    assert(tableNames.contains(SampleDatabase.TABLE_WITH_PART_COL))
    val tableWithPartCol = tables.find { it.tableName == SampleDatabase.TABLE_WITH_PART_COL }!!
    assert(compareCols(tableWithPartCol, sampleDatabase.tableWithPartCols))
    assert(comparePartCols(tableWithPartCol, sampleDatabase.tableWithPartCols))

    assert(tableNames.contains(SampleDatabase.TABLE_WITH_SORT_COL))
    val tableWithSortCol = tables.find { it.tableName == SampleDatabase.TABLE_WITH_SORT_COL }!!
    assert(compareCols(tableWithSortCol, sampleDatabase.tableWithSortCols))
    assert(compareSortCols(tableWithSortCol, sampleDatabase.tableWithSortCols))

    assert(tableNames.contains(SampleDatabase.TABLE_WITH_BUCKET_COL))
    val tableWithBucketCols = tables.find { it.tableName == SampleDatabase.TABLE_WITH_BUCKET_COL }!!
    assert(compareCols(tableWithBucketCols, sampleDatabase.tableWithBucketCols))
    assert(tableWithBucketCols.sd.bucketCols.toSet() == sampleDatabase.tableWithBucketCols.sd.bucketCols.toSet() )
  }

  @Test
  fun `Partition stats serialization`() {
    metadataExtractor.extractPartitionStats(SampleDatabase.DB, listOf(SampleDatabase.TABLE_WITH_PART_COL))
    val actualFile = File("${conf.outputDirectory}/partition/testdb_partition.json")
    val stats = objectMapper.readValue(actualFile, Map::class.java)
    assert(stats == mapOf("${SampleDatabase.DB}.${SampleDatabase.TABLE_WITH_PART_COL}" to 1))
  }

  private fun compareCols(t1: Table, t2: Table) =
      compare(t1.sd.cols, t2.sd.cols)

  private fun comparePartCols(t1: Table, t2: Table) =
      compare(t1.partitionKeys, t2.partitionKeys)

  private fun compare(partCols1: MutableList<FieldSchema>, partCols2: MutableList<FieldSchema>) =
      partCols1.map { it.name }.toSet() == partCols2.map { it.name }.toSet()

  private fun compareSortCols(t1: Table, t2: Table) =
      t1.sd.sortCols.map { it.col }.toSet() == t2.sd.sortCols.map { it.col }.toSet()

}
