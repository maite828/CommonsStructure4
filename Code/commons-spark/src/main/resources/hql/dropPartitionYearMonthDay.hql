alter table $1 set tblproperties ("EXTERNAL"='FALSE');
alter table $1 drop IF EXISTS partition (year=$2, month=$3, day=$4) purge;
alter table $1 set tblproperties ("EXTERNAL"='TRUE');