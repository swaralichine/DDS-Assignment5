package assignment.five

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.json4s.ClassDelta.delta

import scala.io.Source
import spark.io

object AssignmentFive extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession:SparkSession = SparkSession.builder().
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.serializer",classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
    master("local[*]")
    .appName("lastassignment").getOrCreate()

  SedonaSQLRegistrator.registerAll(sparkSession)
  SedonaVizRegistrator.registerAll(sparkSession)

  val resourseFolder = System.getProperty("user.dir")+"/src/test/resources/"
  val csvPolygonInputLocation = resourseFolder + "testenvelope.csv"
  val csvPointInputLocation = resourseFolder + "testpoint.csv"
  val firstpointdata = resourseFolder + "outputdata/firstpointdata"
  val newpointdata = resourseFolder + "outputdata/myTempData"
  val firstpolydata = resourseFolder + "outputdata/firstpolygondata"
  val newpolydata = resourseFolder + "outputdata/firstpolygondata"

  println("Q2.1")
  firstPointQuery()
  println("Q2.2")
  secondPointQuery()
  println("Q2.3")
  firstPloygonQuery()
  println("Q2.4")
  secondPolygonQuery()
  println("Q2.5")
  JoinQuery()

  println("Assignment Five Done!!!")

//  val read_format = "delta"
//  val write_format = "delta"
//  val load_path = "/databricks-datasets/learning-spark-v2/people/people-10m.delta"
//  val save_path = "/tmp/delta/people-10m"
//  val table_name = "default.people10m"

  def firstPointQuery(): Unit = {
    //Read the given testpoint.csv file in csv format and write in delta format and save named firstpointdata
//    var csv_read = Source.fromFile(csvPointInputLocation)


      var inp_buf = sparkSession.read.csv(csvPointInputLocation)

    // Write the data to its target.
      inp_buf.write.format("delta").mode("overwrite").save(firstpointdata)

//    println(firstpointdata)
  }

  def secondPointQuery(): Unit = {
    //Read the firstpointdata in delta format. Print the total count of the points.
    var read_parquet = sparkSession.read.format("delta").load(firstpointdata)
    read_parquet.count()
    println(read_parquet.count())
  }

  def firstPloygonQuery(): Unit = {
    //Read the given testenvelope.csv in csv format and write in delta format and save it named firstpolydata
    var inp_buf = sparkSession.read.csv(csvPolygonInputLocation)

    inp_buf.write.format("delta").mode("overwrite").save(firstpolydata)

  }

  def secondPolygonQuery(): Unit = {
    //Read the firstpolydata in delta format. Print the total count of the polygon
    var read_parquet = sparkSession.read.format("delta").load(firstpolydata)
    read_parquet.count()
    println(read_parquet.count())
  }

  def JoinQuery(): Unit = {
    //Read the firstpointdata in delta format and find the total count for point pairs where distance between the points within a pair is less than 2.
    var read_points = sparkSession.read.format("delta").load(firstpointdata)
    read_points = read_points.toDF()
    read_points.createOrReplaceTempView("points")
    read_points = sparkSession.sql("select ST_Point(cast(points._c0 as Decimal(24,20)), cast(points._c1 as Decimal(24,20))) as point from points")
    //read_points.show()
    read_points.createOrReplaceTempView("pointsDF")
    var joinPoints = sparkSession.sql("select count(*) from pointsDF p1 join pointsDF p2 where p1.point != p2.point and ST_Distance(p1.point, p2.point) < 2")
    joinPoints.show()
  }
}