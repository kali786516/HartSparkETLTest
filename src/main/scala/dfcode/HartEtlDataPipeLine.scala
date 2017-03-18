package dfcode

/**
  * Created by kalit_000 on 17/03/2017.
  */

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import config.Settings

object HartEtlDataPipeLine {

  def main(args: Array[String]) {

    val starttime=System.currentTimeMillis()
    /*set job name in yarn*/
    val jobname="HartEtlDataPipeLine"
    var logger = Logger.getLogger(this.getClass())

    println(s"At least 1 argument need to be passed")

    if (args.length < 1)
    {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: " +
        "SparkMaster:- Local[2] or yarn-cluster "

      )
      System.exit(1)
    }

    /*filter extra logging in spark op*/

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /*check whether the ide is IntelliJ IDEA*/
    val isIDE={
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }

    val deploymode=args(0)


    val conf = new SparkConf().setMaster(deploymode).setAppName("Hart_ETL_PipeLine").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    /*if code running in intelliJ ide runt he code in local*/

    if (isIDE)
    {
      conf.setMaster("local[*]")
    }

    /*get settings application conf file*/
    val hdp=Settings.PipeLineConfig


    val aol_data_set_path=hdp.aol_data_set_path
    val death_by_city_path=hdp.death_by_city_path
    val cities_local_data_path=hdp.cities_local_data_path
    val hart_zookeeper_info=hdp.hart_zookeeper_info
    val hart_solr_collection=hdp.hart_solr_collection


    /*Task 1:- filter out aol data where ClickURL value is empty*/

    val dfAol = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      //.option("inferSchema", "true") // Automatically infer data types
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .option("escape", ":")
      .option("parserLib","univocity")
      .option("delimiter", "\t")
      .load(aol_data_set_path)

    val AolQuery=sqlContext.sql("select Query,ItemRank from aol_data_set where ClickURL !=''").persist(StorageLevel.MEMORY_AND_DISK)

   /*create data set for task 2 :-Deaths_in_122_U.S._cities_-_1962-2016._122_Cities_Mortality_Reporting_System.csv*/

    val dfDeaths = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      //.option("inferSchema", "true") // Automatically infer data types
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .option("escape", ":")
      .option("parserLib","univocity")
      .option("delimiter", ",")
      .load(death_by_city_path)

    /*create data set for task 3 :-500_Cities__Local_Data_for_Better_Health.csv*/

    val dfCitiesLocalData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      //.option("inferSchema", "true") // Automatically infer data types
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .option("escape", ":")
      .option("parserLib","univocity")
      .option("delimiter", ",")
      .load(cities_local_data_path)

    /*create sql temp table for cities_local_data*/
    dfCitiesLocalData.registerTempTable("cities_local_data")

    /*create sql temp table for deaths in us by city*/
    dfDeaths.registerTempTable("deaths_in_us_by_city")

    /*apply our business logic*/
    val dataCities=sqlContext.sql(s"" +
      "with deaths_by_state_and_year as " +
      "( " +
      "select " +
              "Year ," +
              "State ," +
              "sum(`All Deaths`) as number_of_deaths " +
      "from " +
              "deaths_in_us_by_city " +
      "group by " +
               "Year,State " +
      ") " +
      "select " +
              "a.Short_Question_Text," +
               "cast(b.Year as string) Year," +
               "b.State," +
               "cast(b.number_of_deaths as string) number_of_deaths " +
      "from " +
               "cities_local_data a " +
      "join " +
               "deaths_by_state_and_year b " +
      "on " +
               "a.StateAbbr=b.State " +
      "where " +
      "        a.StateAbbr !=''" +
      "and" +
      "         b.State !=''" +
      "").persist(StorageLevel.MEMORY_AND_DISK)


    AolQuery.registerTempTable("aol_query_filtered")
    dataCities.registerTempTable("death_and_cities")

    val finalOp=sqlContext.sql("" +
      "select " +
              "a.*,b.* " +
      "from " +
      "       aol_query_filtered a " +
      "join " +
      "       death_and_cities as b " +
      "on  " +
      "       a.Query=b.Short_Question_Text " +
      "order by " +
      "       ItemRank desc " +
      "limit 10 ")

    finalOp.registerTempTable("final_etl_tmp_table")

    /*################################################################################################################*/
    /*####################################Store to Hive###############################################################*/
    /*################################################################################################################*/
    val hiveCtx =new HiveContext(sc)
    import hiveCtx._

    /*create hive parquet table if not exists by using schema from our output*/
    hiveCtx.sql("CREATE TABLE if not exists hart_etl_table stored as parquet select * from final_etl_tmp_table where 1=2;")

    /*insert into hart_etl_table from final_etl_tmp_table*/
    hiveCtx.sql("insert into table hart_etl_table select * from final_etl_tmp_table;")

    /*################################################################################################################*/
    /*####################################save to solr collection#####################################################*/
    /*################################################################################################################*/

    var writeToSolrOpts = Map("zkhost" -> hart_zookeeper_info, "collection" -> hart_solr_collection)

    /*write to solr*/
    finalOp.write.format("solr").options(writeToSolrOpts).save


    /*calculate end time of scala class*/
    val endTime=System.currentTimeMillis()
    val totalTime=endTime-starttime

    println("JobName "+ jobname +" took (%s".format(totalTime/1000d) + ") seconds to process ")

    sc.stop()
    /*Return exit code 0 if all the above steps are successful*/
    System.exit(0)

  }


}
