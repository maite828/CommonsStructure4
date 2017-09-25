package com.carrefour.ingestion.commons.repository.manage.queries

trait CommonsAlterQueries {
  /**
    *
    * @param a table
    * @return query
    */
  abstract def sQuerySetTableAsExternal(a: String): String = {
    s"alter table $a set tblproperties ('EXTERNAL'='TRUE')"
  }

  /**
    *
    * @param a table
    * @return query
    */
  def sQuerySetTableAsInternal(a: String): String = {
    s"alter table $a set tblproperties ('EXTERNAL'='FALSE')"
  }

}
