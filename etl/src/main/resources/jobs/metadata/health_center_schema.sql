drop table if exists hivmigration_health_center;

create table hivmigration_health_center (
	hiv_emr_id int PRIMARY KEY,
  openmrs_id int NOT NULL
);

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 0, location_id from location where name = 'Unknown Location';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 3, location_id from location where name = 'Centre de Santé Saint-Michel de Boucan-Carré';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 4, location_id from location where name = 'Hôpital Bon Sauveur de Cange';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 5, location_id from location where name = 'Hôpital Notre-Dame de la Nativité de Belladère';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 6, location_id from location where name = 'Hôpital Sainte-Thérèse de Hinche';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 7, location_id from location where name = 'Centre de Santé de Thomonde';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 8, location_id from location where name = 'Hôpital la Colline de Lascahobas';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 21, location_id from location where name = 'Centre de Santé de Cerca la Source';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 22, location_id from location where name = 'SSPE de Saint-Marc';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 33, location_id from location where name = 'Hôpital Universitaire de Mirebalais';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 37, location_id from location where name = 'Centre Médical Charles Colimon de Petite Riviere de l''Artibonite';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 42, location_id from location where name = 'Unknown Location';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 63, location_id from location where name = 'Hôpital Dumarsais Estimé de Verrettes';

insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
select 82, location_id from location where name = 'Hôpital Saint-Nicolas de Saint-Marc';

## TODO:
## Review all of the below and change to valid locations as appropriate in OpenMRS
## Also review which of these locations might be able to be deleted or merged in HIV EMR

# TODO Cerca cavajal
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 31, location_id from location where name = 'Unknown Location';

# TODO Thomassique
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 32, location_id from location where name = 'Unknown Location';

# TODO Savanette
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 34, location_id from location where name = 'Unknown Location';

# TODO Baptiste
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 35, location_id from location where name = 'Unknown Location';

# TODO Maissade
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 36, location_id from location where name = 'Unknown Location';

# TODO Tilory
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 38, location_id from location where name = 'Unknown Location';

# TODO Dufailly
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 39, location_id from location where name = 'Unknown Location';

# TODO Promotion Objectif Zerosida (POZ)
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 40, location_id from location where name = 'Unknown Location';

# TODO Jean Denis
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 122, location_id from location where name = 'Unknown Location';

# TODO Jean Denis
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 123, location_id from location where name = 'Unknown Location';

# TODO Jean Denis
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 142, location_id from location where name = 'Unknown Location';

# TODO Thomassique
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 162, location_id from location where name = 'Unknown Location';

# TODO Cerca Cavarjal
insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
  select 163, location_id from location where name = 'Unknown Location';
