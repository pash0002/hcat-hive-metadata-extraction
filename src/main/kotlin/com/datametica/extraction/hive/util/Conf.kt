package com.datametica.extraction.hive.util

import java.util.*

class Conf(properties: Properties) {
  val thriftURL: String = properties.getProperty("thriftUrl")
  val outputDirectory: String = properties.getProperty("outputDirectory")
  val kerberosEnabled: Boolean = properties.getProperty("kerberos.enabled")!!.toBoolean()
  val hiveKerberosPrincipal: String? = properties.getProperty("kerberos.hive.principal")
  val beautifyJson: Boolean? = properties.getProperty("beautifyJson")!!.toBoolean()
  val sasl: Boolean = properties.getProperty("sasl")!!.toBoolean()
  val useSubjectCredsOnly: String = properties.getProperty("javax.security.auth.useSubjectCredsOnly")
}
