
CREATE PROCEDURE create_patient (_source_patient_id INT)
BEGIN

  select      p.person_uuid, p.pih_id, p.nif_id, p.national_id,
              concat(p.first_name, if(p.first_name2 is null, '', concat(' ', p.first_name2))), p.last_name,
              p.gender, p.birthdate, ifnull(p.birthdate_estimated, 0), p.phone_number, p.birth_place, p.accompagnateur_name,
              u.user_id, p.patient_created_date
  into        @person_uuid, @pih_id, @nif_id, @national_id,
              @first_name, @last_name,
              @gender, @birthdate, @birthdate_estimated, @phone_number, @birth_place, @accompanateur_name,
              @patient_created_by, @patient_created_date
  from        hivmigration_patients p
  left join   hivmigration_users hu on p.patient_created_by = hu.source_user_id
  left join   users u on u.uuid = hu.user_uuid
  where       source_patient_id = _source_patient_id;

  INSERT INTO person(gender, birthdate, birthdate_estimated, creator, date_created, uuid)
  VALUES (@gender, @birthdate, @birthdate_estimated, @patient_created_by, @patient_created_date, @person_uuid);

  SELECT person_id into @person_id FROM person WHERE uuid = @person_uuid;

  INSERT INTO person_name(person_id, given_name, family_name, preferred, creator, date_created, uuid)
  VALUES (@person_id, @first_name, @last_name, 1, @patient_created_by, @patient_created_date, uuid());

  INSERT INTO patient(patient_id, creator, date_created)
  VALUES (@person_id, @patient_created_by, @patient_created_date);

  SELECT patient_identifier_type_id into @hiv_emr_id_type from patient_identifier_type where name = 'HIVEMR-V1';
  SELECT location_id into @identifier_location from location where name = 'Unknown Location';

  INSERT INTO patient_identifier(patient_id, identifier, identifier_type, location_id, preferred, creator, date_created, uuid)
  VALUES (@person_id, _source_patient_id, @hiv_emr_id_type, @identifier_location, 1, @patient_created_by, @patient_created_date, uuid());

END

/*
TODO:   p.pih_id, p.nif_id, p.national_id, , p.phone_number, p.birth_place, p.accompagnateur_name
 */