package utils

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.SparkContext

/**
  * Created by peerslee on 17-4-26.
  */
class MongoSQL(sc : SparkContext) {

  def put(schemaStr : String, rowRdd : RDD[Row], collection : String): Unit = {

    val sqlContext = SQLContext.getOrCreate(sc)

    val schema = StructType(schemaStr.split(",")
      .map(column => StructField(column, StringType, true)))
    val df = sqlContext.createDataFrame(rowRdd, schema)
    MongoSpark.save(df.write.option("collection", collection).mode("append"))
  }
}
