package workouts
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,lit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object Lab16_phoenix_read {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab10-foreachbatch").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
   
    val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transdatatopic")
    .load().selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")
    
    val df1 =  df.select(split(df("value"),",").alias("trans"))
    
    //Phoenix table should already there
    val df2 = df1.select(df1("trans").getItem(0).alias("TXNID").cast(IntegerType),
        df1("trans").getItem(1).alias("TXNDATE"),
        df1("trans").getItem(2).alias("CUSTID").cast(IntegerType),
        df1("trans").getItem(3).alias("AMOUNT").cast(FloatType),
        df1("trans").getItem(4).alias("CATEGORY"),
        df1("trans").getItem(5).alias("PRODUCT"),
        df1("trans").getItem(6).alias("CITY"),
        df1("trans").getItem(7).alias("STATE"),
        df1("trans").getItem(8).alias("PAYTYPE"))
    
    df2.writeStream
    .foreachBatch(writetophoenix)
    .outputMode("append")
    .start()
    .awaitTermination()
    
  }
  val writetophoenix = (df: DataFrame, batchId: Long) => 
    {
      //val df1 = df.withColumn("Batch", lit(batchId))
      
     df.write.format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "TRANSDATA")
      .option("zkUrl", "localhost:2181")
      .save()
      println("written into hbase")
    }
}