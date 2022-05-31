package sample

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}
import java.util.logging.{Level, Logger}
import scala.Console.println

object Principal {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val ss = SparkSession
      .builder()
      .appName("Test1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val path1: String = "C:\\Users\\josem\\OneDrive\\Escritorio\\PRACTICAS TERALCO\\Spark\\Practica7v1\\parquets\\parquet1\\part-00000-18800b2e-2f80-4566-915c-a345a9873c79-c000.snappy.parquet"
    val df1: DataFrame = ss.read.parquet(path1)

    val path2: String = "C:\\Users\\josem\\OneDrive\\Escritorio\\PRACTICAS TERALCO\\Spark\\Practica7v1\\parquets\\parquet3\\gf_file_version_desc=1\\part-00000-bec1b249-fafd-4ea7-89e2-81d6455c7797-c000.snappy.parquet"
    val df2: DataFrame = ss.read.parquet(path2)

    println(comparaColumnas(df1, df2))
    safeUnion(df1, df2).show(50, false)
    getRegistrosDuplicados(df1, Seq("g_rptg_neocon_society_id", "gf_entity_id")).show(50, false)
  }

  //METODO 1
  def comparaColumnas(df1: DataFrame, df2: DataFrame): Tuple3[Seq[String], Seq[String], Seq[String]] = {
    val col1 = df1.columns
    val col2 = df2.columns
    var columnasEnLosDosDataFrame: Seq[String] = col1.intersect(col2)
    val columnasSoloEnPrimerDataFrame: Seq[String] = col1.filter(a => !col2.contains(a))
    val columnasSoloEnSegundoDataFrame: Seq[String] = col2.filter(a => !col1.contains(a))
    Tuple3(columnasEnLosDosDataFrame, columnasSoloEnPrimerDataFrame, columnasSoloEnSegundoDataFrame)
  }

  //Metodo 2
  def safeUnion(df1: DataFrame, df2: DataFrame): DataFrame = {
    val col1 = df1.columns
    val col2 = df2.columns
    var columnasEnLosDosDataFrame: Seq[String] = col1.intersect(col2)
    val columnasSoloEnPrimerDataFrame: Seq[String] = col1.filter(a => !col2.contains(a))
    val columnasSoloEnSegundoDataFrame: Seq[String] = col2.filter(a => !col1.contains(a))
    var columnasComunes: DataFrame = df2.select(columnasEnLosDosDataFrame.head, columnasEnLosDosDataFrame.tail:_*)
    val casoBaseUnionDf2: DataFrame = columnasComunes.withColumn(columnasSoloEnPrimerDataFrame.head, lit(null))
    val safeUnionFoldLeft = columnasSoloEnPrimerDataFrame.tail
      .foldLeft(casoBaseUnionDf2)((acum, columnName) => acum.withColumn(columnName, lit(null)))
    val dfFinal: DataFrame = df1.select(col1.map(col):_*).union(safeUnionFoldLeft.select(col1.map(col):_*))
    dfFinal
  }

  //Metodo 3
  def getRegistrosDuplicados(df1: DataFrame, campos: Seq[String]): DataFrame = {
    val columnaHead = campos.head
    val columnaTail = campos.tail
    val salida = df1.groupBy(columnaHead, columnaTail:_*).agg(functions.count("*").as("NUM_APARICIONES"))
    salida
  }
}

