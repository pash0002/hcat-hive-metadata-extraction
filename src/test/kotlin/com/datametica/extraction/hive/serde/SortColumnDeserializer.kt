package com.datametica.extraction.hive.serde

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import org.apache.hadoop.hive.metastore.api.Order
import java.io.IOException

class SortColumnDeserializer @JvmOverloads constructor(vc: Class<*>? = null) : StdDeserializer<Order?>(vc) {

  @Throws(IOException::class, JsonProcessingException::class)
  override fun deserialize(jp: JsonParser, ctxt: DeserializationContext?): Order {
    val node: JsonNode = jp.codec.readTree(jp)
    val col = node.get("col").asText()
    val order: Int = node.get("order").asInt()
    return Order(col, order)
  }

}