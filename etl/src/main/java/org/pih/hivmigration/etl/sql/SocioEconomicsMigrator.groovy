package org.pih.hivmigration.etl.sql

class SocioEconomicsMigrator extends ObsMigrator {
    @Override
    def void migrate() {
        // hivmigration_socioeconomics and hivmigration_socioeconomics_extra loaded by StagingTablesMigrator

        create_tmp_obs_table()

        executeMysql("Create socioeconomics encounters", '''            
            insert into encounter
                (encounter_datetime, date_created, encounter_type, form_id, patient_id, creator, location_id, uuid)
            select intake.encounter_date,
                   intake.date_created,
                   (select encounter_type_id from encounter_type where name = 'Socio-economics'),
                   (select form_id from form where name = 'Socioeconomics Note'),
                   p.person_id,
                   1,
                   ifnull(intake.location_id, 1),
                   uuid()
            from hivmigration_socioeconomics s
            join hivmigration_patients p on s.patient_id = p.source_patient_id
            join hivmigration_encounters intake on p.source_patient_id = intake.source_patient_id and intake.source_encounter_type = 'intake\'
            where s.num_rooms_in_house is not null
               or s.num_people_in_house is not null
               or s.latrine_p is not null
               or s.radio_p is not null
               or s.education is not null
               or s.type_of_floor is not null
               or s.type_of_roof is not null;
               
            insert into encounter
                (encounter_datetime, date_created, encounter_type, form_id, patient_id, creator, location_id, uuid)
            select intake.encounter_date,
                   intake.date_created,
                   (select encounter_type_id from encounter_type where name = 'Socio-economics'),
                   (select form_id from form where name = 'Socioeconomics Note'),
                   p.person_id,
                   1,
                   ifnull(intake.location_id, 1),
                   uuid()
            from hivmigration_socioeconomics_extra s
            join hivmigration_patients p on s.patient_id = p.source_patient_id
            join hivmigration_encounters intake on p.source_patient_id = intake.source_patient_id and intake.source_encounter_type = 'intake\'
            where s.method_of_transport is not null
               or s.walking_time_to_clinic is not null
               or s.time_of_transport is not null
               or s.arrival_method_other is not null;
        ''')

        executeMysql("Add encounter IDs to socioeconomics and socioeconomics_extra tables", '''
            ALTER TABLE hivmigration_socioeconomics ADD COLUMN socioecon_encounter_id INT;            
            ALTER TABLE hivmigration_socioeconomics_extra ADD COLUMN intake_encounter_id INT;
            
            -- Add socioeconomics encounter ids to socioeconomics
            UPDATE hivmigration_socioeconomics hs
            JOIN encounter e
                ON hs.patient_id = e.patient_id
                AND e.encounter_type = (select encounter_type_id from encounter_type where name = 'Socio-economics')
            SET hs.socioecon_encounter_id = e.encounter_id;

            -- Add intake encounter ids to socioeconomics_extra
            UPDATE hivmigration_socioeconomics_extra hse
            JOIN hivmigration_encounters he
                ON hse.patient_id = he.patient_id
                AND he.source_encounter_type = 'intake'
            SET hse.intake_encounter_id = he.encounter_id;
        ''')

        executeMysql("Migrate fields from socioeconomics table into socioeconomics form", '''
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_numeric)
            SELECT socioecon_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1474'),
                   num_people_in_house
            FROM hivmigration_socioeconomics
            WHERE num_people_in_house IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_numeric)
            SELECT socioecon_encounter_id,
                   concept_uuid_from_mapping('CIEL', '1475'),
                   num_rooms_in_house
            FROM hivmigration_socioeconomics
            WHERE num_rooms_in_house IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT socioecon_encounter_id,
                   concept_uuid_from_mapping('PIH', 'ROOF MATERIAL'),
                   CASE type_of_roof
                       WHEN 'concrete' THEN concept_uuid_from_mapping('PIH', 'CEMENT')
                       WHEN 'sheet' THEN concept_uuid_from_mapping('PIH', 'SHEET METAL')
                       WHEN 'thatch' THEN concept_uuid_from_mapping('PIH', 'THATCH')
                       WHEN 'tile' THEN concept_uuid_from_mapping('CIEL', '159679')  -- ceramic tile (not on new form)
                       WHEN 'tin' THEN concept_uuid_from_mapping('PIH', 'SHEET METAL')
                       END
            FROM hivmigration_socioeconomics
            WHERE type_of_roof IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT socioecon_encounter_id,
                   concept_uuid_from_mapping('PIH', 'ROOF MATERIAL'),
                   CASE type_of_floor
                       WHEN 'beaten ground' THEN concept_uuid_from_mapping('PIH', 'BEATEN EARTH')
                       WHEN 'cement' THEN concept_uuid_from_mapping('PIH', 'CEMENT')
                       WHEN 'dirt' THEN concept_uuid_from_mapping('PIH', 'OTHER')
                       WHEN 'mud' THEN concept_uuid_from_mapping('PIH', 'OTHER')
                       END
            FROM hivmigration_socioeconomics
            WHERE type_of_floor IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT socioecon_encounter_id,
                   concept_uuid_from_mapping('PIH', 'Latrine'),
                   IF( latrine_p = 't',
                       concept_uuid_from_mapping('PIH', 'YES'),
                       concept_uuid_from_mapping('PIH', 'NO'))
            FROM hivmigration_socioeconomics
            WHERE latrine_p IN ('t', 'f');
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT socioecon_encounter_id,
                   concept_uuid_from_mapping('PIH', '1318'),
                   IF( radio_p = 't',
                       concept_uuid_from_mapping('PIH', 'YES'),
                       concept_uuid_from_mapping('PIH', 'NO'))
            FROM hivmigration_socioeconomics
            WHERE radio_p IN ('t', 'f');
        ''')

        executeMysql("Migrate fields from socioeconomics_extra table into intake form", '''
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT intake_encounter_id,
                   concept_uuid_from_mapping('CIEL', '163731'),
                   IF( smokes_p = 't',
                       concept_uuid_from_mapping('PIH', 'CURRENTLY'),
                       concept_uuid_from_mapping('PIH', 'NO'))
            FROM hivmigration_socioeconomics_extra
            WHERE smokes_p IN ('t', 'f');
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_numeric)
            SELECT intake_encounter_id,
                   concept_uuid_from_mapping('CIEL', '159931'),
                   trim(smoking_years)
            FROM hivmigration_socioeconomics_extra
            WHERE smoking_years IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_numeric)
            SELECT intake_encounter_id,
                   concept_uuid_from_mapping('PIH', '11949'),
                   trim(num_cigarretes_per_day)
            FROM hivmigration_socioeconomics_extra
            WHERE num_cigarretes_per_day IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT intake_encounter_id,
                   concept_uuid_from_mapping('CIEL', '159449'),
                   concept_uuid_from_mapping('PIH', 'CURRENTLY')
            FROM hivmigration_socioeconomics_extra
            WHERE num_days_alcohol_per_week > 0 OR beer_per_day > 0 OR wine_per_day > 0 OR drinks_per_day > 0;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_numeric)
            SELECT intake_encounter_id,
                   concept_uuid_from_mapping('PIH', '2243'),
                   trim(num_days_alcohol_per_week)
            FROM hivmigration_socioeconomics_extra
            WHERE num_days_alcohol_per_week IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_numeric)
            SELECT intake_encounter_id,
                   concept_uuid_from_mapping('PIH', '2246'),
                   (IFNULL(trim(wine_per_day), 0) + IFNULL(trim(beer_per_day), 0) + IFNULL(trim(drinks_per_day), 0))
            FROM hivmigration_socioeconomics_extra
            WHERE wine_per_day IS NOT NULL OR beer_per_day IS NOT NULL OR drinks_per_day IS NOT NULL;
        ''')

        executeMysql("Migrate fields from socioeconomics_extra table into socioeconomics form", '''
            SET @socioecon_encounter_type = (select encounter_type_id from encounter_type where name = 'Socio-economics');
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT e.encounter_id,
                   concept_uuid_from_mapping('PIH', '975'),
                   CASE method_of_transport
                       WHEN ' bus' THEN concept_uuid_from_mapping('PIH', 'MINI BUS')
                       WHEN 'bicycle' THEN concept_uuid_from_mapping('PIH', 'BY BICYCLE')
                       WHEN 'car' THEN concept_uuid_from_mapping('PIH', 'CAR')
                       WHEN 'foot' THEN concept_uuid_from_mapping('PIH', 'WALKING')
                       WHEN 'horse' THEN concept_uuid_from_mapping('PIH', '980')
                       WHEN 'horse_mule' THEN concept_uuid_from_mapping('PIH', '980')
                       WHEN 'minibus' THEN concept_uuid_from_mapping('PIH', 'MINI BUS')
                       WHEN 'other' THEN concept_uuid_from_mapping('PIH', 'OTHER')
                       WHEN 'stretcher' THEN concept_uuid_from_mapping('PIH', 'STRETCHER')
                       WHEN 'walk' THEN concept_uuid_from_mapping('PIH', 'WALKING')
                       END
            FROM hivmigration_socioeconomics_extra hse
            JOIN encounter e ON hse.patient_id = e.patient_id AND e.encounter_type = @socioecon_encounter_type
            WHERE method_of_transport IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_coded_uuid)
            SELECT e.encounter_id,
                   concept_uuid_from_mapping('PIH', 'CLINIC TRAVEL TIME'),
                   CASE IFNULL(time_of_transport, walking_time_to_clinic)
                        WHEN 'under_30_min' THEN concept_uuid_from_mapping('PIH', 'LESS THAN 30 MINUTES')
                        WHEN '30_min_to_1_hour' THEN concept_uuid_from_mapping('PIH', '30 TO 60 MINUTES')
                        WHEN '1_to_2_hours' THEN concept_uuid_from_mapping('PIH', 'ONE TO TWO HOURS')
                        WHEN '2_to_3_hours' THEN concept_uuid_from_mapping('PIH', '982')  -- 2-3 hours
                        WHEN '3_to_6_hours' THEN concept_uuid_from_mapping('PIH', '3669')  -- >3 hours
                        WHEN 'over_6_hours' THEN concept_uuid_from_mapping('PIH', '3669')  -- >3 hours
                        WHEN 'unknown' THEN concept_uuid_from_mapping('PIH', 'UNKNOWN')
                        END
            FROM hivmigration_socioeconomics_extra hse
            JOIN encounter e ON hse.patient_id = e.patient_id AND e.encounter_type = @socioecon_encounter_type
            WHERE walking_time_to_clinic IS NOT NULL OR time_of_transport IS NOT NULL;
            
            INSERT INTO tmp_obs
                (encounter_id, concept_uuid, value_text)
            SELECT e.encounter_id,
                   concept_uuid_from_mapping('PIH', '1301'),  -- comment
                   arrival_method_other
            FROM hivmigration_socioeconomics_extra hse
            JOIN encounter e ON hse.patient_id = e.patient_id AND e.encounter_type = @socioecon_encounter_type
            WHERE arrival_method_other IS NOT NULL;
        ''')

        migrate_tmp_obs()

    }

    @Override
    def void revert() {

    }
}
