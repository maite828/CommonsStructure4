package com.carrefour.ingestion.commons.service.manage.queries

trait CommonsSelectQueries {
  /**
    *
    * @param a
    * @param b
    * @param c
    * @param d
    * @return
    */
  def sQueryDescribeFormattedPartition(a: Int, b: Int, c: Int, d: Int): String = {
    val query = s"describe formatted $a partition (year=$b, month=$c, day=$d);"
    query
  }

}