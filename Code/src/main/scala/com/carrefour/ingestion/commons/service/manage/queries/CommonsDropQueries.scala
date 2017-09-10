package com.carrefour.ingestion.commons.service.manage.queries

trait CommonsDropQueries {
  /**
    *
    * @param a
    * @param b
    * @param c
    * @param d
    * @return
    */
  def sQueryDropPartitionYearMonthDay(a:Int,b:Int,c:Int,d:Int) :String = {
    s"alter table $a drop IF EXISTS partition (year=$b, month=$c, day=$d)"
  }

}
