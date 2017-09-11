package com.carrefour.ingestion.commons.repository.manage.queries

trait CommonsDropQueries {
  /**
    *
    * @param a table
    * @param b years
    * @param c month
    * @param d day
    * @return query
    */
  abstract def sQueryDropPartitionYearMonthDay(a:String,b:Int,c:Int,d:Int) :String = {
    s"alter table $a drop IF EXISTS partition (year=$b, month=$c, day=$d)"
  }

}
