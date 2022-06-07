package edu.ucr.cs.cs167.echen111

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "prepare" =>
          val ebirdDF = sparkSession.read.format("csv")
            .option("sep", ",")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(inputFile)
//                    ebirdDF.show()
//                    ebirdDF.printSchema()
          val ebirdGeomDF: DataFrame = ebirdDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry")
//          ebirdGeomDF.show()
//          ebirdGeomDF.printSchema()
//          val ebirdRDD: SpatialRDD = ebirdDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry").toSpatialRDD
          val convertedDF: DataFrame = ebirdGeomDF.select("geometry", "x", "y", "GLOBAL UNIQUE IDENTIFIER", "CATEGORY", "COMMON NAME", "SCIENTIFIC NAME", "SUBSPECIES COMMON NAME", "OBSERVATION COUNT", "OBSERVATION DATE")
//          convertedDF.show()
//          convertedDF.printSchema()
          val renamedDF : DataFrame = convertedDF.withColumnRenamed("GLOBAL UNIQUE IDENTIFIER", "GLOBAL_UNIQUE_IDENTIFIER")
            .withColumnRenamed("SCIENTIFIC NAME", "SCIENTIFIC_NAME")
            .withColumnRenamed("SUBSPECIES COMMON NAME", "SUBSPECIES_COMMON_NAME")
            .withColumnRenamed("OBSERVATION COUNT", "OBSERVATION_COUNT")
            .withColumnRenamed("OBSERVATION DATE", "OBSERVATION_DATE")
//          renamedDF.show()
//          renamedDF.printSchema()
          val renamedRDD: SpatialRDD = renamedDF.toSpatialRDD
          val zipRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
          val birdZipRDD: RDD[(IFeature, IFeature)] = renamedRDD.spatialJoin(zipRDD)
          val birdZip: DataFrame = birdZipRDD.map({ case (bird, zip) => Feature.append(bird, zip.getAs[String]("ZCTA5CE10"), "ZIPCode") })
            .toDataFrame(sparkSession)
//          birdZip.show()
//          birdZip.printSchema()
          val birdZipDropGeom: DataFrame = birdZip.drop("geometry")
          birdZipDropGeom.printSchema()
          birdZipDropGeom.show()
//          birdZipDropGeom.write.mode(SaveMode.Overwrite).parquet("eBird_ZIP")


//          val tweetsByCounty: Map[String, Long] = countyTweet
//            .map({ case (county, tweet) => (county.getAs[String]("NAME"), 1) })
//            .countByKey()
//          println("County\tCount")
//          for ((county, count) <- tweetsByCounty)
//            println(s"$county\t$count")
//        case "convert" =>
//          val outputFile = args(2)
//          // TODO add a CountyID column to the tweets, parse the text into keywords, and write back as a Parquet file
//          val tweetsDF = sparkSession.read.format("csv")
//            .option("sep", "\t")
//            .option("inferSchema", "true")
//            .option("header", "true")
//            .load(inputFile)
//          val tweetsRDD: SpatialRDD = tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry").toSpatialRDD
//          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
//          val tweetCountyRDD: RDD[(IFeature, IFeature)] = tweetsRDD.spatialJoin(countiesRDD)
//          val tweetCounty: DataFrame = tweetCountyRDD.map({ case (tweet, county) => Feature.append(tweet, county.getAs[String]("GEOID"), "CountyID") })
//            .toDataFrame(sparkSession)
//          //          tweetCounty.printSchema()
//          val convertedDF: DataFrame = tweetCounty.selectExpr("CountyID", "Longitude", "Latitude", "split(lower(text), ',') AS keywords", "Timestamp")
//          //          convertedDF.printSchema()
//          convertedDF.write.mode(SaveMode.Overwrite).parquet(outputFile)
//        case "count-by-keyword" =>
//          val keyword: String = args(2)
//          // TODO count the number of occurrences of each keyword per county and display on the screen
//          sparkSession.read.parquet(inputFile)
//            .createOrReplaceTempView("tweets")
//          println("CountyID\tCount")
//          sparkSession.sql(
//            s"""
//              SELECT CountyID, count(*) AS count
//              FROM tweets
//              WHERE array_contains(keywords, "$keyword")
//              GROUP BY CountyID
//            """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))
//        case "choropleth-map" =>
//          val keyword: String = args(2)
//          val outputFile: String = args(3)
//          // TODO write a Shapefile that contains the count of the given keyword by county
//          sparkSession.read.parquet(inputFile)
//            .createOrReplaceTempView("tweets")
//          println("CountyID\tCount")
//          sparkSession.sql(
//            s"""
//              SELECT CountyID, count(*) AS count
//              FROM tweets
//              WHERE array_contains(keywords, "$keyword")
//              GROUP BY CountyID
//            """).createOrReplaceTempView("keyword_counts")
//          sparkContext.shapefile("tl_2018_us_county.zip")
//            .toDataFrame(sparkSession)
//            .createOrReplaceTempView("counties")
//          sparkSession.sql(
//            s"""
//              SELECT CountyID, NAME, g, count
//              FROM keyword_counts, counties
//              WHERE CountyID = GEOID
//            """).toSpatialRDD
//            .coalesce(1)
//            .saveAsShapefile(outputFile)
        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}