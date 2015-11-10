package com.vngrs.spark.configuration

import java.util.Map.Entry
import com.typesafe.config.{ConfigFactory, ConfigValue}
import com.typesafe.config.ConfigValueType._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

/**
 * Created by sametozcan on 08/11/15.
 */

object Config {

  val properties = ConfigFactory.load("application.properties")
  private val logger = Logger.getLogger(getClass)


  /**
   * Reads spark options from configuration file
   * @param key
   * @return
   */
  private def getPropertiesList(key: String) = {
    val list = Try(config.getConfig(key).entrySet().toArray).getOrElse(Array())
    list.map(x => {
      val p = x.asInstanceOf[Entry[String, ConfigValue]]
      val k = p.getKey

      val v = p.getValue.valueType match {
        case BOOLEAN => config.getBoolean(key + "." + k)
        case STRING => config.getString(key + "." + k)
        case NUMBER => config.getDouble(key + "." + k)
        case _ => config.getString(key + "." + k)
      }
      (k.replace("_", "."), v.toString)
    })
  }

  // Spark configuration
  private val config = ConfigFactory.load(properties.getString("property.file"))
  lazy val SPARK_MASTER_HOST = Try(config.getString("spark.master_host")).getOrElse("local")
  lazy val SPARK_MASTER_PORT = Try(config.getInt("spark.master_port")).getOrElse(7077)
  lazy val SPARK_HOME = Try(config.getString("spark.home")).getOrElse("/spark/home")
  lazy val SPARK_MEMORY = Try(config.getString("spark.memory")).getOrElse("20g")
  lazy val SPARK_OPTIONS = getPropertiesList("spark.options")

  /**
   * Creates Spark Context with configuration read from file
   * @return
   */
  def connectToSparkCluster(): SparkContext = {

    val master = if (SPARK_MASTER_HOST.toUpperCase == "LOCAL") "local" else SPARK_MASTER_HOST + ":" + SPARK_MASTER_PORT

    // Spark Context configuration
    val scConf =
      SPARK_OPTIONS.fold(
        new SparkConf()
					.setMaster(master)
					.setAppName("weatherApp")
					.set("spark.executor.memory", SPARK_MEMORY)
					.setSparkHome(SPARK_HOME)
      )((c, p) => { // apply each spark option from the configuration file in section "spark.options"
      val (k, v) = p.asInstanceOf[(String, String)]
        c.asInstanceOf[SparkConf].set(k, v)
      }).asInstanceOf[SparkConf]
    // Create and return the spark context to be used through the entire KYC application
    new SparkContext(scConf)

  }

}
