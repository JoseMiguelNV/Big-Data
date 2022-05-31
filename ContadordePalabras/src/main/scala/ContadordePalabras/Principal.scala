package ContadordePalabras

import scala.io.Source

class Principal {
  var mapa: Map[String, Int] = Map()
  var lineas: List[String] = null
  var palabras: Tuple2[String, Int] = null
  var contador = 0

  def lecturaFichero(url: String): Unit = {
    val fichero = Source.fromFile(url)
    for(linea <- fichero.getLines()) {
      lineas = linea.replaceAll("\\p{Punct}", "").toLowerCase().split(" ").toList
      imprimir(lineas)
    }
    mapa.foreach(CV => println(CV._1 + " -> " + CV._2))
    fichero.close()
  }

  def imprimir(lineas: List[String]): Unit = {
    for(palabra <- lineas) {
      if (mapa.contains(palabra)) {
        contador = mapa(palabra) + 1
        mapa += (palabra -> contador)
      } else {
        mapa += (palabra -> 1)
      }
    }
  }
}

object Principal {
  def main(args: Array[String]): Unit = {
    val principal = new Principal()
    principal.lecturaFichero(args(0))
  }
}
