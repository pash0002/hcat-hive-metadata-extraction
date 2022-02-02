package com.datametica.extraction.hive.serde

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import org.apache.hadoop.hive.metastore.api.Database
import java.io.IOException

class DatabaseDeserializer @JvmOverloads constructor(vc: Class<*>? = null) : StdDeserializer<Database?>(vc) {

  @Throws(IOException::class, JsonProcessingException::class)
  override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): Database {
    val node: JsonNode = jp.codec.readTree(jp)
    val name = node.get("name").asText()
    val description = node.get("description").asText()
    val locationUri = node.get("locationUri").asText()

    return Database(name, description, locationUri, mapOf())
  }

}