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
            .withColumnRenamed("COMMON NAME", "COMMON_NAME")
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
          birdZipDropGeom.write.mode(SaveMode.Overwrite).parquet("eBird_ZIP")

        case "spatialAnalysis" =>
          // Where Task 2 (Spatial Analysis) starts
          val keyword: String = args(2)

          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("birds")

          sparkSession.sql(
            s"""SELECT ZIPCode, SUM(OBSERVATION_COUNT) AS total
      FROM birds
      GROUP BY ZIPCode
      """).createOrReplaceTempView("totalObservations")

          sparkSession.sql(
            s"""SELECT ZIPCode, SUM(OBSERVATION_COUNT) AS filtered
      FROM birds
      WHERE COMMON_NAME = "$keyword"
      GROUP BY ZIPCode
      """).createOrReplaceTempView("filteredObservations")

          sparkSession.sql(
            s"""SELECT filtered / total AS ratio, totalObservations.ZIPCode
      FROM totalObservations, filteredObservations
      WHERE totalObservations.ZIPCode = filteredObservations.ZIPCode
      """).createOrReplaceTempView("ratios")

          sparkContext.shapefile("tl_2018_us_zcta510.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("zipcodes")

          val result =  sparkSession.sql(
            s"""SELECT ratios.ZIPCode, zipcodes.g, ratios.ratio
      FROM ratios, zipcodes
      WHERE ratios.ZIPCode = zipcodes.ZCTA5CE10
      """)

          result.toSpatialRDD.coalesce(1).saveAsShapefile("eBirdZIPCodeRatio")

        case "pie-chart" =>

          val startingDate: String = args(2)
          val endingDate: String = args(3)
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("birds")
          import org.apache.spark.sql.functions._
          sparkSession.sql(
            s"""
                      SELECT COMMON_NAME, SUM(OBSERVATION_COUNT) AS count
                      FROM birds
                      WHERE TO_DATE(OBSERVATION_DATE, 'yyyy-MM-dd') >= TO_DATE('$startingDate', 'MM/dd/yyyy') AND
                      TO_DATE(OBSERVATION_DATE, 'yyyy-MM-dd') <= TO_DATE('$endingDate', 'MM/dd/yyyy')
                      GROUP BY COMMON_NAME;
                    """)
            .coalesce(1)
            .write.format("csv")
            .option("header", "true")
            .save("eBirdObservationsTime")
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