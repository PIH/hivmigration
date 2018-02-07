drop table if exists hivmigration_providers;

create table hivmigration_providers (
  id int PRIMARY KEY AUTO_INCREMENT,
  hiv_provider_name varchar(256)
);