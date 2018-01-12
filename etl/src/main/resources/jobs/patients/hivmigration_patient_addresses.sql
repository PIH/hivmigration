drop table if exists hivmigration_patient_addresses;

create table hivmigration_patient_addresses (
  address_id int PRIMARY KEY,
  source_patient_id int,
  address_type NVARCHAR(8),
  entry_date date,
  address NVARCHAR(512),
  department NVARCHAR(100),
  commune NVARCHAR(100),
  section NVARCHAR(100),
  locality NVARCHAR(100)

);
