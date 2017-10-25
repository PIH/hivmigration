drop table if exists hivmigration_health_center;

create table hivmigration_health_center (
  location_id int PRIMARY KEY AUTO_INCREMENT,
	health_center_id int,
	name varchar(72)
);

insert into hivmigration_health_center(health_center_id, name) values(3, "BOUCAN_CARRE");
insert into hivmigration_health_center(health_center_id, name) values(4, "CANGE");
insert into hivmigration_health_center(health_center_id, name) values(5, "BELLADERES");
insert into hivmigration_health_center(health_center_id, name) values(6, "HINCHE");
insert into hivmigration_health_center(health_center_id, name) values(7, "THOMONDE");
insert into hivmigration_health_center(health_center_id, name) values(8, "LASCAHOBAS");
insert into hivmigration_health_center(health_center_id, name) values(21, "CERCA_LA_SOURCE");
insert into hivmigration_health_center(health_center_id, name) values(22, "ST_MARC_SSPE");
insert into hivmigration_health_center(health_center_id, name) values(31, "CERCA_CAVAJAL");
insert into hivmigration_health_center(health_center_id, name) values(32, "THOMASSIQUE");
insert into hivmigration_health_center(health_center_id, name) values(33, "MIREBALAIS");
insert into hivmigration_health_center(health_center_id, name) values(34, "SAVANETTE");
insert into hivmigration_health_center(health_center_id, name) values(35, "BAPTISTE");
insert into hivmigration_health_center(health_center_id, name) values(36, "MAISSADE");
insert into hivmigration_health_center(health_center_id, name) values(37, "PETITE_RIVIERE");
insert into hivmigration_health_center(health_center_id, name) values(38, "TILORY");
insert into hivmigration_health_center(health_center_id, name) values(39, "DUFAILLY");
insert into hivmigration_health_center(health_center_id, name) values(40, "POZ");
insert into hivmigration_health_center(health_center_id, name) values(42, "OTHER");
insert into hivmigration_health_center(health_center_id, name) values(63, "VERRETTES");
insert into hivmigration_health_center(health_center_id, name) values(82, "ST_MARC_HSN");