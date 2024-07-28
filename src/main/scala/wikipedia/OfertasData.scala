package ofertas_data

import scala.io.Source
import scala.io.Codec

object OfertasData:

  // Ruta actualizada para el archivo CSV en la carpeta Linkedin
  private[ofertas_data] def lines: List[String] =
    Option(getClass.getResourceAsStream("/Linkedin/ofertas_data.csv")) match
      case None => sys.error("guarda el fichero ofertas_data.csv en la ruta src/main/resources/Linkedin")
      case Some(resource) => Source.fromInputStream(resource)(Codec.UTF8).getLines().toList

  // Método parse actualizado para manejar el formato ignorando los separadores
  private[ofertas_data] def parse(line: String): OfertasDataArticle =
    // Asignar un título genérico o derivado del contenido
    val title = "Generic Title" // O puedes derivarlo de alguna manera, por ejemplo, usar un índice
    val text = line.trim
    OfertasDataArticle(title, text)