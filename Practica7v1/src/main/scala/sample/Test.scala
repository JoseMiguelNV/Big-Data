package sample

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit, when}
import org.scalatest.funsuite.AnyFunSuite

class Test extends AnyFunSuite {

  val ss = SparkSession
    .builder()
    .appName("Test1")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val path1: String = "C:\\Users\\josem\\OneDrive\\Escritorio\\PRACTICAS TERALCO\\Spark\\Practica7v1\\parquets\\parquet1\\part-00000-18800b2e-2f80-4566-915c-a345a9873c79-c000.snappy.parquet"
  val df1: DataFrame = ss.read.parquet(path1)

  val path2: String = "C:\\Users\\josem\\OneDrive\\Escritorio\\PRACTICAS TERALCO\\Spark\\Practica7v1\\parquets\\parquet3\\gf_file_version_desc=1\\part-00000-bec1b249-fafd-4ea7-89e2-81d6455c7797-c000.snappy.parquet"
  val df2: DataFrame = ss.read.parquet(path2)

  df1.show(100, false)
  df2.show(100, false)

  println(s"El parquet 1 tiene ${df1.count()} filas y ${df1.columns.length} columnas.")
  println(s"El parquet 2 tiene ${df2.count()} filas y ${df2.columns.length} columnas.")

  val col1 = df1.columns
  val col2 = df2.columns

  var columnasEnLosDosDataFrame: Seq[String] = col1.intersect(col2)
  val columnasSoloEnPrimerDataFrame: Seq[String] = col1.filter(a => !col2.contains(a))
  val columnasSoloEnSegundoDataFrame: Seq[String] = col2.filter(a => !col1.contains(a))


  test("comparaColumnas") {
    val col1 = df1.columns
    val col2 = df2.columns
    var columnasEnLosDosDataFrame: Seq[String] = col1.intersect(col2)
    println(columnasEnLosDosDataFrame)
    val columnasSoloEnPrimerDataFrame: Seq[String] = col1.filter(a => !col2.contains(a))
    println(columnasSoloEnPrimerDataFrame)
    val columnasSoloEnSegundoDataFrame: Seq[String] = col2.filter(a => !col1.contains(a))
    println(columnasSoloEnSegundoDataFrame)
  }

  test("safeUnion") {
    //Se obtienen las columnas de los dos dataframes para obtener el caso base
    var columnasComunes: DataFrame = df2.select(columnasEnLosDosDataFrame.head, columnasEnLosDosDataFrame.tail:_*)
    //Caso base
    val casoBaseUnionDf2: DataFrame = columnasComunes.withColumn(columnasSoloEnPrimerDataFrame.head, lit(null))

    //Definir foldLeft
    val safeUnionFoldLeft = columnasSoloEnPrimerDataFrame.tail
      .foldLeft(casoBaseUnionDf2)((acum, columnName) => acum.withColumn(columnName, lit(null)))

    //Realizar la union
    val dfFinal: DataFrame = df1.select(col1.map(col):_*).union(safeUnionFoldLeft.select(col1.map(col):_*))
    dfFinal.show(50, false)
  }

  test("getRegistrosDuplicados") {
    val listaCampos: Seq[String] = Seq("g_rptg_neocon_society_id", "gf_entity_id")
    val columnaHead = listaCampos.head
    val columnaTail = listaCampos.tail
    val salida = df1.groupBy(columnaHead, columnaTail:_*).agg(functions.count("*").as("NUM_APARICIONES"))
    salida.show(100,false)
  }
}
