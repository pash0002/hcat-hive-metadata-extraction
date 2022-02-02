package com.datametica.extraction.hive.serde

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Order
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import java.io.IOException

class TableDeserializer @JvmOverloads constructor(vc: Class<*>? = null) : StdDeserializer<Table?>(vc) {

  companion object {
    val objectMapper: ObjectMapper =
        ObjectMapper()
            .registerModule(SimpleModule()
                .addDeserializer(Order::class.java, SortColumnDeserializer())
                .addDeserializer(FieldSchema::class.java, ColumnDeserializer()))
  }

  @Throws(IOException::class, JsonProcessingException::class)
  override fun deserialize(jp: JsonParser, ctx: DeserializationContext?): Table {
    val node: JsonNode = jp.codec.readTree(jp)
    val dbName = node.get("dbName").asText()
    val tableName: String = node.get("tableName").asText()
    val partitionKeys = objectMapper.readValue(node.get("partitionKeys").toString(), Array<FieldSchema>::class.java)
    val sdNode = node.get("sd")
    val cols = objectMapper.readValue(sdNode.get("cols").toString(), Array<FieldSchema>::class.java)
    val sortCols = objectMapper.readValue(sdNode.get("sortCols").toString(), Array<Order>::class.java)
    val bucketCols = objectMapper.readValue(sdNode.get("bucketCols").toString(), Array<String>::class.java)
    val sd = StorageDescriptor()
    sd.cols = cols.toList()
    sd.bucketCols = bucketCols.toList()
    sd.sortCols = sortCols.toList()
    val table = Table()
    table.dbName = dbName
    table.tableName = tableName
    table.sd = sd
    table.partitionKeys = partitionKeys.toList()
    return table
  }

}