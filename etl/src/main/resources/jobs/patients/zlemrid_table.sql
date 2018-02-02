drop table if exists hivmigration_zlemrid;

create table hivmigration_zlemrid (
  person_id int PRIMARY KEY AUTO_INCREMENT,
  zl_emr_id varchar(6)
);