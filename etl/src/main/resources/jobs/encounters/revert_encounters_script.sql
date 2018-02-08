
delete from encounter where uuid in (select encounter_uuid from hivmigration_encounters);
