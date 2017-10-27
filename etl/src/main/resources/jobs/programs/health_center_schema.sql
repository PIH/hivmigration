drop table if exists hivmigration_health_center;

create table hivmigration_health_center (
    id int PRIMARY KEY AUTO_INCREMENT,
	health_center_id int,
	location_id int,
	name varchar(72)
);

insert into hivmigration_health_center(health_center_id, location_id, name) values(3, 3, "BOUCAN_CARRE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(4, 4, "CANGE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(5, 2, "BELLADERES");
insert into hivmigration_health_center(health_center_id, location_id, name) values(6, 6, "HINCHE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(7, 11, "THOMONDE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(8, null, "LASCAHOBAS");
insert into hivmigration_health_center(health_center_id, location_id, name) values(21, 5, "CERCA_LA_SOURCE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(22, 10, "ST_MARC_SSPE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(31, null, "CERCA_CAVAJAL");
insert into hivmigration_health_center(health_center_id, location_id, name) values(32, null, "THOMASSIQUE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(33, null, "MIREBALAIS");
insert into hivmigration_health_center(health_center_id, location_id, name) values(34, null, "SAVANETTE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(35, null, "BAPTISTE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(36, null, "MAISSADE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(37, 9, "PETITE_RIVIERE");
insert into hivmigration_health_center(health_center_id, location_id, name) values(38, null, "TILORY");
insert into hivmigration_health_center(health_center_id, location_id, name) values(39, null, "DUFAILLY");
insert into hivmigration_health_center(health_center_id, location_id, name) values(40, null, "POZ");
insert into hivmigration_health_center(health_center_id, location_id, name) values(42, 1, "OTHER");
insert into hivmigration_health_center(health_center_id, location_id, name) values(63, 12, "VERRETTES");
insert into hivmigration_health_center(health_center_id, location_id, name) values(82, 7, "ST_MARC_HSN");