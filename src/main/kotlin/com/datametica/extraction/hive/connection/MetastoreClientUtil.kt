package com.datametica.extraction.hive.connection

import com.datametica.extraction.hive.util.Conf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hive.hcatalog.common.HCatUtil
import org.apache.logging.log4j.kotlin.Logging

object MetastoreClientUtil: Logging {

  private val hiveConf =  HiveConf(MetastoreClientUtil::class.java)

  fun createClient(conf: Conf): IMetaStoreClient {
    logger.debug("Creating IMetaStoreClient.")
    return getClient(conf)
  }

  private fun initiateKerberization(conf: Conf) {
    logger.debug("Setting kerberos related config.")
    logger.debug("SASL: ${conf.sasl}")
    logger.debug("kerberosPrincipal: ${conf.hiveKerberosPrincipal}")
    logger.debug("useSubjectCredsOnly: ${conf.useSubjectCredsOnly}")
    hiveConf.set("hive.metastore.local", "false")
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, conf.sasl)
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, conf.hiveKerberosPrincipal)
    System.setProperty("javax.security.auth.useSubjectCredsOnly", conf.useSubjectCredsOnly)
  }

  private fun getClient(conf: Conf): IMetaStoreClient {
    logger.debug("thriftURL: ${conf.thriftURL}")
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, conf.thriftURL)
    logger.debug("Kerberos enabled: ${conf.kerberosEnabled}")
    if (conf.kerberosEnabled) {
      initiateKerberization(conf)
    }
    return create(Configuration(hiveConf))
  }

  private fun create(conf: Configuration): IMetaStoreClient {
    try {
      logger.info("Creating HMS client.")
      return HCatUtil.getHiveMetastoreClient(HCatUtil.getHiveConf(conf))
    } catch (e: Exception) {
      throw RuntimeException("Exception while creating HMS client.", e)
    }
  }

}
