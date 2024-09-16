package org.apache.spark.callback

import java.util.Collections
import java.util.concurrent.CompletableFuture
import com.microsoft.aad.msal4j.{IAuthenticationResult, _}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback




class AuthBySecretCallBackWithParamsPySpark(params: scala.collection.immutable.Map[String, Object]) extends AadAuthenticationCallback {

  implicit def toJavaFunction[A, B](f: Function1[A, B]) = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }

  override def authority: String = params.get("authority") match {
    case None => ""
    case Some(obj) => obj.asInstanceOf[String]
  }

  val clientId: String = params.get("clientId") match {
    case None => ""
    case Some(obj) => obj.asInstanceOf[String]
  }

  val clientSecret: String = params.get("clientSecret") match {
    case None => ""
    case Some(obj) => obj.asInstanceOf[String]
  }

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    val app = AuthBySecretCallBackWithParamsPySpark.getClientApp(clientId, clientSecret, authority)

    val parameters = ClientCredentialParameters.builder(Collections.singleton(audience + ".default")).build

    app.acquireToken(parameters).thenApply((result: IAuthenticationResult) => result.accessToken())
  } catch {
    case e: Exception =>
      val failed = new CompletableFuture[String]
      failed.completeExceptionally(e)
      failed
  }
}

object AuthBySecretCallBackWithParamsPySpark {

  @volatile private var app: ConfidentialClientApplication = _

  def getClientApp(clientId: String, clientSecret: String, authority: String): ConfidentialClientApplication = {
    if (app == null) {
      synchronized {
        if (app == null) {
          // 初始化 app 單例變量
          app = ConfidentialClientApplication
            .builder(clientId, ClientCredentialFactory.createFromSecret(clientSecret))
            .authority("https://login.microsoftonline.com/" + authority)
            .build()
        }
      }
    }
    app
  }
}