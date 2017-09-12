package com.carrefour.ingestion.commons.service

import com.carrefour.ingestion.commons.bean.IngestionMetadata
import com.carrefour.ingestion.commons.controller.IngestionSettings

trait MetadataService{

  def loadMetadata(settings: IngestionSettings): Array[IngestionMetadata]
}
