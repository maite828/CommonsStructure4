package com.carrefour.ingestion.commons.service.manage.queries

trait CommonsShowQueries {
  /**
    *
    * @param a
    * @param b
    * @param c
    * @param d
    * @return
    */
  def sQueryDescribeFormattedPartition(a:Int,b:Int,c:Int,d:Int) :String = {
    val query = s"describe formatted $a partition (year=$b, month=$c, day=$d);"
    query
  }














  def sQueryInsertOverwriteBackUpTable(bckDb: String, bckTable: String, sPartition: String, campos: String,
                                       consDb: String, consTable: String, sWhere: String): String = {
    //s"INSERT OVERWRITE TABLE $bckDb.$bckTable PARTITION ($sPartition) SELECT $campos FROM $consDb.$consTable WHERE $sWhere"
    s"INSERT OVERWRITE TABLE $bckDb.$bckTable PARTITION ($sPartition) SELECT $campos FROM $consDb.$consTable"
  }

  def sQueryInsertOverwriteTableFromQuery(qualifiedTableName: String, sPartition: String,
                                          campos: String, originDb: String, originTable: String,
                                          sWhere: String):
  String = {
    if (isEmpty(originDb)) {
      s"INSERT OVERWRITE TABLE $qualifiedTableName PARTITION ($sPartition) " +
        s"SELECT $campos FROM $originTable WHERE $sWhere"

    } else {
      s"INSERT OVERWRITE TABLE $qualifiedTableName PARTITION ($sPartition ) " +
        s"SELECT $campos FROM $originDb.$originTable WHERE $sWhere"
    }
  }

  def isEmpty(x: String): Boolean = x == null || x.trim.isEmpty

}
