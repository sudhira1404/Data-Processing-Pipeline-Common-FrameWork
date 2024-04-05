package com.tgt.dse.mdf.common.pipeline.util

import com.tgt.dse.mdf.common.pipeline.LoggingTrait
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.{CredentialProvider, CredentialProviderFactory}

object CredentialUtils extends LoggingTrait  {

  /**
   * Will get Credentials From Jcek based on jceKeyName
   *
   * @param jceKeyStorePath                    jceKeyStorePath
   * @param jceKeyName                         jceKeyName
   */

  def getCredentialFromJcek(jceKeyStorePath: String, jceKeyName: String): String = {
    try {
      log.info(s"Getting credentials from jcek filepath $jceKeyStorePath for the key $jceKeyName")
      val conf = new Configuration()
      conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceKeyStorePath)
      val credentialProvider: CredentialProvider = CredentialProviderFactory.getProviders(conf).get(0)
      credentialProvider.getCredentialEntry(jceKeyName).getCredential.mkString
    } catch {
      case e: Throwable =>
        throw new Exception("Exception in getCredentialFromJcek :-" + e)
    }
  }

}
