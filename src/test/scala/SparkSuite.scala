package am

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.scalatest.Suite

import java.io.File

trait SparkSuite extends Suite {
  val propertiesUrl = new File("src/test/resources/log4j.properties").toURI.toURL
  PropertyConfigurator.configure(propertiesUrl)
  lazy implicit val spark: SparkSession = {
    System.setSecurityManager(null)
    SparkSuite()
  }

}

object SparkSuite {
  def apply(
             defaultBuilder: Builder = SparkSession.builder(),
             customConf: Set[(String, String)] = Set.empty
           ): SparkSession = {
    val builder = defaultBuilder
      .appName("test")
      .config("spark.master", "local[1,4]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.task.maxFailures", "4")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.session.timeZone", "UTC")
    val ss = customConf
      .foldLeft[Builder](builder) { case (b: Builder, (k: String, v: String)) => b.config(k, v) }
      .getOrCreate()
    sys.addShutdownHook {
      ss.close()
    }
    ss
  }
}