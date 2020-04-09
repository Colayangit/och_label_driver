package cn.dfcx.label.maincode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOptTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val session: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("dfsdfsd")
      .enableHiveSupport()
      .getOrCreate()


    val dataFrame: DataFrame = session.sql(
      """
        select * from dwd.dwd_order_info
      """.stripMargin)
    dataFrame.show()

    session.stop()
  }
}
