package org.apache.hive.hcatalog.api

import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Order
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema

class SampleDatabase(private val hmsClient: IMetaStoreClient) {
  companion object {
    const val DB = "testdb"
    const val SIMPLE_TABLE = "simple_table"
    const val TABLE_WITH_SORT_COL = "table_with_sort_cols"
    const val TABLE_WITH_PART_COL = "table_with_part_cols"
    const val TABLE_WITH_BUCKET_COL = "table_with_bucket_cols"
    const val ID = "id"
    const val NAME = "name"
  }

  private val stringTypeInfo = PrimitiveTypeInfo()
  private val intTypeInfo = PrimitiveTypeInfo()
  lateinit var simpleTable: Table
  lateinit var tableWithPartCols: Table
  lateinit var tableWithSortCols: Table
  lateinit var tableWithBucketCols: Table

  init {
    stringTypeInfo.typeName = HCatFieldSchema.Type.STRING.name.toLowerCase()
    intTypeInfo.typeName = HCatFieldSchema.Type.INT.name.toLowerCase()
  }

  fun create(): SampleDatabase {
    createDb()
    simpleTable = createSimpleTable()
    tableWithPartCols = createTableWithPartCols()
    tableWithSortCols = createTableWithSortCols()
    tableWithBucketCols = createTableWithBucketCols()
    return this
  }

  private fun createSimpleTable(): Table {
    val id = HCatFieldSchema(ID, intTypeInfo, null)
    val name = HCatFieldSchema(NAME, stringTypeInfo, null)
    return createTable(SIMPLE_TABLE, listOf(id, name), emptyList(), emptyList(), emptyList())
  }

  private fun createTableWithSortCols(): Table {
    val id = HCatFieldSchema(ID, intTypeInfo, null)
    val name = HCatFieldSchema(NAME, stringTypeInfo, null)
    val city = Order(NAME, 1)
    return createTable(TABLE_WITH_SORT_COL, listOf(id, name), emptyList(), listOf(city), emptyList())
  }

  private fun createTableWithPartCols(): Table {
    val id = HCatFieldSchema(ID, intTypeInfo, null)
    val name = HCatFieldSchema(NAME, stringTypeInfo, null)
    val state = HCatFieldSchema("state", stringTypeInfo, null)
    val table = createTable(TABLE_WITH_PART_COL, listOf(id, name), listOf(state), emptyList(), emptyList())
    createPartition(table)
    return table
  }

  private fun createPartition(table: Table) {
    val firstPtn = mapOf("state" to "CA")
    val hCatPartition = HCatPartition(HCatTable(table), firstPtn, "/user/hive/warehouse/")
    val addPtn = HCatAddPartitionDesc.create(hCatPartition).build()
    hmsClient.add_partition(addPtn.hCatPartition.toHivePartition())
  }

  private fun createTableWithBucketCols(): Table {
    val id = HCatFieldSchema(ID, intTypeInfo, null)
    val name = HCatFieldSchema(NAME, stringTypeInfo, null)
    return createTable(TABLE_WITH_BUCKET_COL, listOf(id, name), emptyList(), emptyList(), listOf(ID))
  }

  private fun createTable(
      tableName: String,
      cols: List<HCatFieldSchema>,
      partCols: List<HCatFieldSchema>,
      sortCols: List<Order>,
      bucketCols: List<String>): Table {
    val table = HCatTable(DB, tableName)
        .cols(cols)
        .partCols(partCols)
        .sortCols(sortCols)
        .bucketCols(bucketCols)
        .fileFormat("rcfile")
    val tableDesc = HCatCreateTableDesc.create(table).build()
    val hiveTable = tableDesc.hCatTable.toHiveTable()
    hmsClient.createTable(hiveTable)
    return hiveTable
  }

  private fun createDb() {
    hmsClient.dropDatabase(DB, true, true, true)
    val dbDesc = HCatCreateDBDesc.create(DB).ifNotExists(false)
        .build()
    hmsClient.createDatabase(dbDesc.toHiveDb())
  }

}