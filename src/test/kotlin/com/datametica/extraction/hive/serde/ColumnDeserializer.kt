package com.datametica.extraction.hive.serde

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import org.apache.hadoop.hive.metastore.api.FieldSchema
import java.io.IOException

class ColumnDeserializer @JvmOverloads constructor(vc: Class<*>? = null) : StdDeserializer<FieldSchema?>(vc) {

  @Throws(IOException::class, JsonProcessingException::class)
  override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): FieldSchema {
    val node: JsonNode = jp.codec.readTree(jp)
    val name = node.get("name").asText()
    val type: String = node.get("type").asText()
    val comment: String = node.get("comment").asText()
    return FieldSchema(name, type, comment)
  }

}