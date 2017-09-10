package com.carrefour.ingestion.commons.service.manage.queries

trait CommonsAlterQueries {
  /**
    *
    * @param a
    * @return
    */
  def sQuerySetTableAsExternal(a:String) :String = {
    s"alter table $a set tblproperties ('EXTERNAL'='TRUE')"
  }

  /**
    *
    * @param a
    * @return
    */
  def sQuerySetTableAsInternal(a:String) :String = {
    s"alter table $a set tblproperties ('EXTERNAL'='FALSE')"
  }

}
