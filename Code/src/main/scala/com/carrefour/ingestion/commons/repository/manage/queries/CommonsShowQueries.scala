package com.carrefour.ingestion.commons.repository.manage.queries

trait CommonsShowQueries {
  /**
    *
    * @param a table
    * @param b years
    * @param c month
    * @param d day
    * @return query
    */
  abstract def sQueryDescribeFormattedPartition(a:String,b:Int,c:Int,d:Int) :String = {
    val query = s"describe formatted $a partition (year=$b, month=$c, day=$d);"
    query
  }

}
