package config
import com.typesafe.config.ConfigFactory
/**
  * Created by kalit_000 on 17/03/2017.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object Settings {
  private val config=ConfigFactory.load()

  object PipeLineConfig {
    private val PipeLineConfig=config.getConfig("datapipeline")

    lazy val aol_data_set_path=PipeLineConfig.getString("aol_data_set_path")
    lazy val death_by_city_path=PipeLineConfig.getString("death_by_city_path")
    lazy val cities_local_data_path=PipeLineConfig.getString("cities_local_data_path")
    lazy val hart_zookeeper_info=PipeLineConfig.getString("hart_zookeeper_info")
    lazy val hart_solr_collection=PipeLineConfig.getString("hart_solr_collection")
    lazy val aol_data_delimeter=PipeLineConfig.getString("aol_data_delimeter")
    lazy val deaths_by_city_delimeter=PipeLineConfig.getString("deaths_by_city_delimeter")
    lazy val cities_by_local_delimeter=PipeLineConfig.getString("cities_by_local_delimeter")

  }
}
