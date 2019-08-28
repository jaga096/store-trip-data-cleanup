import com.datastax.spark.connector.toRDDFunctions
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import model.GuestStoreInteractions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author jagadeeshnm 2019-08-21
  */
object StoreTripDataCleanupApp {


  val conf: Config = ConfigFactory.load()
  val sourceKeyspace: String = conf.getString("guestengage.keyspace")
  val SourceTable: String = conf.getString("guestengage.source.table")

  def main(args: Array[String]): Unit = {
    val logger = Logger("store-trip-cleanup")
    val conf = new SparkConf(true).setAppName("APP_NAME").setMaster("local[*]")
    val sparkSession = SparkSession.builder.appName("scala Spark SQL data sources example").config(conf).getOrCreate

    val guestStoreVisitDf = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> SourceTable, "keyspace" -> sourceKeyspace))
      .load
      .select("target_guid","store_visit_date", "order_type", "store_visit_store_id")

    val alphaNumStoreIdDf = guestStoreVisitDf.filter("store_visit_store_id rlike  '[a-zA-Z]+'")

    logger.info("Total number of records having alphanumeric storeid's ------>" + alphaNumStoreIdDf.count())

    val alphaNumericStoreIdDf = alphaNumStoreIdDf.filter(alphaNumStoreIdDf("target_guid").isNotNull &&
      alphaNumStoreIdDf("store_visit_date").isNotNull && alphaNumStoreIdDf("order_type").isNotNull && alphaNumStoreIdDf("store_visit_store_id").isNotNull)

    alphaNumericStoreIdDf.rdd.map(row => GuestStoreInteractions(
      row.getString(0)
      ,row.getString(1)
      ,row.getString(2)
      ,row.getString(3).replaceAll("[^0-9]+","")
    )).saveToCassandra(sourceKeyspace,SourceTable)

    logger.info("Total number of records will be deleted ------>" + alphaNumericStoreIdDf.count())

    alphaNumericStoreIdDf.rdd.map(row => GuestStoreInteractions(
      row.getString(0)
      ,row.getString(1)
      ,row.getString(2)
      ,row.getString(3)
    )).deleteFromCassandra(sourceKeyspace, SourceTable)

  }
}
