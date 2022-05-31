package sample

import org.apache.spark.{SparkConf, SparkContext}

import java.util.logging.{Level, Logger}

object Principal {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)
    val conf = new SparkConf().setAppName("Prueba1").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lineas = sc.textFile("C:\\Users\\josem\\OneDrive\\Escritorio\\PruebasSpark\\ficheros\\RealState.csv")


  }
}

