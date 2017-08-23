import com.carrefour.ingestion.commons.loader.{FileLoader, IngestionSettings, IngestionSettingsLoader}
import com.carrefour.ingestion.commons.util.SparkUtils

object SsffLoaderDriver {
  def main(args: Array[String]): Unit = {
    IngestionSettingsLoader.parse(args, IngestionSettings()).fold(ifEmpty = throw new IllegalArgumentException("Invalid configuration")) {
      settings => {
        settings.businessunit = "Ssff"
        SparkUtils.withHiveContext("Servicios Financieros ssff relational data loader") { implicit sqlContext => FileLoader.run(settings) }
      }
    }
  }
}
