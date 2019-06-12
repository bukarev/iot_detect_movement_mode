import java.net._
import java.io._
import scala.io._

import scala.util.matching.Regex

import org.apache.spark._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.mapr.db.spark.sql._
import org.apache.spark.sql._
import com.mapr.db.spark.streaming._

import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.sql.types._


import net.liftweb.json._
import net.liftweb.json.Serialization.write

import scala.util.parsing.json.JSONObject

import org.apache.spark.ml._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

object azureSensortag {

def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
}

  def main(args: Array[String]) {

case class rawSignal(deviceId: String, offset: String, enqueuedTime: String, sequenceNumber: String, content: String)
case class iotSignal(deviceId: String, msgID: String, utctime: String, gyroX: String, gyroY: String, gyroZ: String, accX: String, accY: String, accZ: String)   

val Array(
              brokers,
              topics,
              groupId,
              offsetReset,
              batchInterval,
              pollTimeout,
              mode,
              winLength,
              winSlide) = args

val topicsSet = topics.split(",").toSet

val spark = SparkSession
  .builder
  .appName("azureSensorTag")
  .getOrCreate()

import spark.implicits._

val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval.toInt) )

val model = PipelineModel.read.load("/user/sensortag/models/model_save4")
        
val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> brokers,
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> groupId,
  "auto.offset.reset" -> offsetReset,
  "enable.auto.commit" -> (false: java.lang.Boolean),
  "failOnDataLoss" -> (false: java.lang.Boolean)
)

val consumerStrategy =
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy)
      
    val messages1 = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy)               

    val analyseThis = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy)
                   
val jDeviceId = "deviceId=(.*?),".r
val jOffset = "offset=(.*?),".r
val jEnqueuedTime = "enqueuedTime=(.*?),".r
val jSequenceNumber = "sequenceNumber=(.*?),".r
val jContent = "content=(.*?)},".r

implicit val formats = DefaultFormats

val getDeviceId = udf((x: String) => (jDeviceId findFirstIn x).mkString.replace("deviceId=","").dropRight(1))
val getOffset = udf((x: String) => (jOffset findFirstIn x).mkString.replace("offset=","").dropRight(1))
val getEnqTime = udf((x: String) => (jEnqueuedTime findFirstIn x).mkString.replace("enqueuedTime=","").dropRight(1))
val getSeqNumber = udf((x: String) => (jSequenceNumber findFirstIn x).mkString.replace("sequenceNumber=","").dropRight(1).toInt)
val getContent = udf((x: String) => (jContent findFirstIn x).mkString.replace("content=","").dropRight(1))

val getGyroX = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroX.toDouble }) 
val getGyroY = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroY.toDouble })
val getGyroZ = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroZ.toDouble })
val getAccX = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accX.toDouble }) 
val getAccY = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accY.toDouble })
val getAccZ = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accZ.toDouble })
val getMsgID = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].msgID })
val getUtcTime = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].utctime })

messages
.transform { rdd =>
      // It's possible to get each input rdd's offset ranges, BUT...
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetReset
      println(s"Offset Reset: ${offsetReset}")
      //println(s"Window slide: ${winSlide.toLong}")
      //println(offsetRanges.mkString("\n"))
      println(s"Size of rdd: ${rdd.count()}")
      //println(s"number of kafka partitions before windowing: ${offsetRanges.size}")
      //println(s"number of spark partitions before windowing: ${rdd.partitions.size}")
      rdd
    }
    .map(x => x.value()).window(new Duration(winLength.toLong), new Duration(winSlide.toLong))
    .foreachRDD(inRDD => {
        //val inRDD = winRDD.coalesce(1)

        println(s"number of spark partitions after windowing: ${inRDD.partitions.size}")
        println(s"Size: ${inRDD.count()}")
        
        val inDF = inRDD.toDF()
                        .withColumn("deviceId", getDeviceId(col("value")))
                        .withColumn("enqueuedTime", getEnqTime(col("value")))
                        .withColumn("sequenceNumber", getSeqNumber(col("value")))
                        .withColumn("content", getContent(col("value")))
                        .withColumn("msgID", getMsgID(col("content")))
                        .withColumn("utcTime", getUtcTime(col("content")))
                        .withColumn("gyroX", getGyroX(col("content")))
                        .withColumn("gyroY", getGyroY(col("content")))
                        .withColumn("gyroZ", getGyroZ(col("content")))
                        .withColumn("accX", getAccX(col("content")))
                        .withColumn("accY", getAccY(col("content")))
                        .withColumn("accZ", getAccZ(col("content")))
                        .select("deviceId", "msgID", "utcTime", "sequenceNumber", "gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ")
                        
          inDF.createOrReplaceTempView("inDF")

        val lagDF = spark.sqlContext.sql("select deviceId, sequenceNumber, utcTime, gyroX, gyroY, gyroZ, accX, accY, accZ, lag(gyroX,1) over(order by sequenceNumber) as gyroX1, lag(gyroY,1) over(order by sequenceNumber) as gyroY1, lag(gyroZ,1) over(order by sequenceNumber) as gyroZ1, lag(accX,1) over(order by sequenceNumber) as accX1, lag(accY,1) over(order by sequenceNumber) as accY1, lag(accZ,1) over(order by sequenceNumber) as accZ1, lag(gyroX,2) over(order by sequenceNumber) as gyroX2, lag(gyroY,2) over(order by sequenceNumber) as gyroY2, lag(gyroZ,2) over(order by sequenceNumber) as gyroZ2, lag(accX,2) over(order by sequenceNumber) as accX2, lag(accY,2) over(order by sequenceNumber) as accY2, lag(accZ,2) over(order by sequenceNumber) as accZ2, lag(gyroX,3) over(order by sequenceNumber) as gyroX3, lag(gyroY,3) over(order by sequenceNumber) as gyroY3, lag(gyroZ,3) over(order by sequenceNumber) as gyroZ3, lag(accX,3) over(order by sequenceNumber) as accX3, lag(accY,3) over(order by sequenceNumber) as accY3, lag(accZ,3) over(order by sequenceNumber) as accZ3, lag(gyroX,4) over(order by sequenceNumber) as gyroX4, lag(gyroY,4) over(order by sequenceNumber) as gyroY4, lag(gyroZ,4) over(order by sequenceNumber) as gyroZ4, lag(accX,4) over(order by sequenceNumber) as accX4, lag(accY,4) over(order by sequenceNumber) as accY4, lag(accZ,4) over(order by sequenceNumber) as accZ4, lag(gyroX,5) over(order by sequenceNumber) as gyroX5, lag(gyroY,5) over(order by sequenceNumber) as gyroY5, lag(gyroZ,5) over(order by sequenceNumber) as gyroZ5, lag(accX,5) over(order by sequenceNumber) as accX5, lag(accY,5) over(order by sequenceNumber) as accY5, lag(accZ,5) over(order by sequenceNumber) as accZ5, lag(gyroX,6) over(order by sequenceNumber) as gyroX6, lag(gyroY,6) over(order by sequenceNumber) as gyroY6, lag(gyroZ,6) over(order by sequenceNumber) as gyroZ6, lag(accX,6) over(order by sequenceNumber) as accX6, lag(accY,6) over(order by sequenceNumber) as accY6, lag(accZ,6) over(order by sequenceNumber) as accZ6, lag(gyroX,7) over(order by sequenceNumber) as gyroX7, lag(gyroY,7) over(order by sequenceNumber) as gyroY7, lag(gyroZ,7) over(order by sequenceNumber) as gyroZ7, lag(accX,7) over(order by sequenceNumber) as accX7, lag(accY,7) over(order by sequenceNumber) as accY7, lag(accZ,7) over(order by sequenceNumber) as accZ7, lag(gyroX,8) over(order by sequenceNumber) as gyroX8, lag(gyroY,8) over(order by sequenceNumber) as gyroY8, lag(gyroZ,8) over(order by sequenceNumber) as gyroZ8, lag(accX,8) over(order by sequenceNumber) as accX8, lag(accY,8) over(order by sequenceNumber) as accY8, lag(accZ,8) over(order by sequenceNumber) as accZ8, lag(gyroX,9) over(order by sequenceNumber) as gyroX9, lag(gyroY,9) over(order by sequenceNumber) as gyroY9, lag(gyroZ,9) over(order by sequenceNumber) as gyroZ9, lag(accX,9) over(order by sequenceNumber) as accX9, lag(accY,9) over(order by sequenceNumber) as accY9, lag(accZ,9) over(order by sequenceNumber) as accZ9 from inDF")
        lagDF.createOrReplaceTempView("lagDF")

        val  mlDF = spark.sqlContext.sql("select * from lagDF where not accZ9 is null")        

        val assembler = new VectorAssembler()
                            .setInputCols(Array("gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ", "gyroX1", "gyroY1", "gyroZ1", "accX1", "accY1", "accZ1", "gyroX2", "gyroY2", "gyroZ2", "accX2", "accY2", "accZ2", "gyroX3", "gyroY3", "gyroZ3", "accX3", "accY3", "accZ3", "gyroX4", "gyroY4", "gyroZ4", "accX4", "accY4", "accZ4", "gyroX5", "gyroY5", "gyroZ5", "accX5", "accY5", "accZ5", "gyroX6", "gyroY6", "gyroZ6", "accX6", "accY6", "accZ6", "gyroX7", "gyroY7", "gyroZ7", "accX7", "accY7", "accZ7", "gyroX8", "gyroY8", "gyroZ8", "accX8", "accY8", "accZ8", "gyroX9", "gyroY9", "gyroZ9", "accX9", "accY9", "accZ9"))
                            .setOutputCol("features")
   
        val output = assembler.transform(mlDF)
        val dataDF = output.select("deviceId", "sequenceNumber", "utcTime", "features")

// Make predictions.
        val predictions = model.transform(dataDF)
        predictions.select("deviceId", "sequenceNumber", "utcTime", "prediction").collect.foreach(println)
        
        predictions.select("deviceId", "sequenceNumber", "utcTime", "prediction") //.orderBy($"deviceId",$"sequenceNumber")
                   .foreachPartition { partitionOfRecords =>
                      try
                       {
                val s = new Socket(InetAddress.getByName("ddsauhdpmr101.dds.au.nttdata.com"), 19999)
                val out = new PrintStream(s.getOutputStream())
                partitionOfRecords.foreach(record => out.println(record))
                out.flush()
                s.close()
             }
           } // end of foreachpartition
        
           })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
