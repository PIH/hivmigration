package org.pih.hivmigration.etl.sql

/**
 * Loads the HIV_ENCOUNTERS table into an intermediate table hivmigration_encounters.
 * Creates encounters for all of the encounter types that are mapped below.
 * Encounters of type 'note' are migrated by NoteMigrator.
 */
class EncounterMigrator extends SqlMigrator {

    void migrate() {
        executeMysql("Create encounter staging table", '''
            create table hivmigration_encounters (
              encounter_id int PRIMARY KEY AUTO_INCREMENT,
              patient_id int,
              source_creator_id int,
              source_encounter_id int,
              source_patient_id int,
              encounter_type_id int,
              source_encounter_type varchar(100),
              location_id int,
              source_location_id int,
              form_id int,
              creator int,
              encounter_uuid char(38),
              encounter_date date,
              performed_by varchar(100),
              date_created datetime,
              comments varchar(4000),
              note_title varchar(100),
              response_to int,
              form_version varchar(10),
              KEY `patient_id_idx` (`patient_id`),
              KEY `source_patient_id_idx` (`source_patient_id`),
              KEY `source_encounter_id_idx` (`source_encounter_id`),
              KEY `source_creator_id_idx` (`source_creator_id`),
              KEY `source_location_id_idx` (`source_location_id`),
              KEY `source_encounter_type_idx` (`source_encounter_type`),
              KEY `encounter_date_idx` (`encounter_date`),
              UNIQUE KEY `uuid_idx` (`encounter_uuid`)
            );
            
        ''')

        setAutoIncrement('hivmigration_encounters', '(select max(encounter_id)+1 from encounter)')

        loadFromOracleToMySql(
                '''
                insert into hivmigration_encounters(source_encounter_id,source_patient_id,source_encounter_type,source_creator_id,encounter_date,date_created,comments,performed_by,source_location_id,note_title,response_to, form_version)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
                '''
                select
                    e.encounter_id as source_encounter_id,
                    e.patient_id as source_patient_id,
                    e.type,
                    e.entered_by,
                    e.encounter_date,
                    e.entry_date,
                    e.comments,
                    e.performed_by,
                    e.encounter_site,
                    e.note_title,
                    e.response_to,
                    COALESCE(i.FORM_VERSION,f.FORM_VERSION,'1')
                from
                    hiv_encounters e, hiv_demographics_real p, HIV_INTAKE_FORMS i, HIV_FOLLOWUP_FORMS f
                    where e.patient_id = p.patient_id and i.ENCOUNTER_ID (+)= e.ENCOUNTER_ID and f.ENCOUNTER_ID (+) = e.ENCOUNTER_ID
            '''
        )
        executeMysql("Add UUIDs", '''
            UPDATE hivmigration_encounters SET encounter_uuid = uuid();
        ''')

        executeMysql("Fill encounter types column", '''            
            UPDATE hivmigration_encounters SET encounter_type_id = CASE
                WHEN source_encounter_type = "intake" THEN encounter_type('HIV Intake')
                WHEN source_encounter_type = "followup" THEN encounter_type('HIV Followup')
                WHEN source_encounter_type = "lab_result" THEN encounter_type('Laboratory Results')
                WHEN source_encounter_type = "anlap_lab_result" THEN encounter_type('Laboratory Results')
                WHEN source_encounter_type = "accompagnateur" THEN encounter_type('HIV drug dispensing')
                WHEN source_encounter_type = "regime" THEN encounter_type('Drug Order Documentation')
                END
        ''')
        // TODO: Handle source_encounter_type "anlap_vital_signs", "patient_contact", "food_study" (https://pihemr.atlassian.net/browse/UHM-3244)

        executeMysql("Fill form id column", '''
            SET @form_intake_v1 = (select form_id from form where uuid = '29a109f6-6dd3-4e98-af88-134cf7e7da1b');
            SET @form_intake_v2 = (select form_id from form where uuid = '12f9aebe-0882-4211-a61e-006e4c7a5a96');
            SET @form_intake_v3 = (select form_id from form where uuid = '53725d05-99c1-4fad-be82-c1ca60a8dd9b');
            SET @form_followup_v1 = (select form_id from form where uuid = '9c180d22-7ef6-49e4-8e52-ff314e218451');
            SET @form_followup_v2 = (select form_id from form where uuid = '60efebcc-a4ff-4459-9754-1eaeafc4dc24');
            SET @form_followup_v3 = (select form_id from form where uuid = 'd2cee145-0b53-4588-9836-dadafa122a8e');
            SET @form_lab_results = (select form_id from form where uuid = '4d778ef4-0620-11e5-a6c0-1697f925ec7b');
            SET @form_hiv_dispensing = (select form_id from form where uuid = 'c3af594f-fd77-44b2-b3a0-44d4f3c7cc3a');
            SET @form_hiv_drug_order = (select form_id from form where uuid = '96482a6e-5b62-11eb-8f5a-0242ac110002');
            
            UPDATE hivmigration_encounters SET form_id = CASE
                WHEN source_encounter_type = "intake" and form_version = 1 THEN @form_intake_v1
                WHEN source_encounter_type = "intake" and form_version = 2 THEN @form_intake_v2
                WHEN source_encounter_type = "intake" and form_version = 3 THEN @form_intake_v3
                WHEN source_encounter_type = "followup" and form_version = 1 THEN @form_followup_v1
                WHEN source_encounter_type = "followup" and form_version = 2 THEN @form_followup_v2
                WHEN source_encounter_type = "followup" and form_version = 3 THEN @form_followup_v3
                WHEN source_encounter_type = "lab_result" THEN @form_lab_results
                WHEN source_encounter_type = "anlap_lab_result" THEN @form_lab_results
                WHEN source_encounter_type = "accompagnateur" THEN @form_hiv_dispensing
                WHEN source_encounter_type = "regime" THEN @form_hiv_drug_order
                END
        ''')

        executeMysql('Set encounter locations',
            '''
            update hivmigration_encounters e, hivmigration_health_center hc
                set e.location_id = hc.openmrs_id         
                where e.source_location_id = hc.hiv_emr_id;
            ''')

        executeMysql("Log warnings about encounters with nonsensical encounter dates",
                '''
                INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type, warning_details, flag_for_review)       
                SELECT p.person_id as patient_id, 
                       e.encounter_id as encounter_id, 
                       e.encounter_date as encounter_date,
                       'encounter_date' as field_name,
                       e.encounter_date as field_value,
                       'Encounter with nonsensical date' as warning_type,
                       CONCAT('Source encounter_type :', e.source_encounter_type),
                       TRUE as flag_for_review
                from hivmigration_encounters e, hivmigration_patients p
                where (e.encounter_date < '1990-01-01' or e.encounter_date > date_add(now(), INTERVAL 5 YEAR)) 
                and e.source_patient_id = p.source_patient_id;  
        ''')
        /*
        executeMysql("Log warnings about encounters with dates before system roll-out date, October 1, 2002",
                '''
                INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type)
                SELECT p.person_id as patient_id,
                       e.encounter_id as encounter_id,
                       e.encounter_date as encounter_date,
                       'encounter_date' as field_name,
                       e.encounter_date as field_value,
                       'Encounter before system roll out' as warning_type
                from hivmigration_encounters e, hivmigration_patients p
                where (e.encounter_date > '1990-01-01' and e.encounter_date < '2002-10-01')
                and e.source_patient_id = p.source_patient_id;
        ''')
        */

        executeMysql("Log warnings about encounters with future dates",
                '''
                INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type, warning_details, flag_for_review)       
                SELECT p.person_id as patient_id, 
                       e.encounter_id as encounter_id, 
                       e.encounter_date as encounter_date,
                       'encounter_date' as field_name,
                       e.encounter_date as field_value,
                       'Encounter in the future' as warning_type,
                       CONCAT('Source encounter_type :', e.source_encounter_type),
                       TRUE as flag_for_review
                from hivmigration_encounters e, hivmigration_patients p
                where (e.encounter_date > now() and e.encounter_date < date_add(now(), INTERVAL 5 YEAR)) 
                and e.source_patient_id = p.source_patient_id;  
        ''')
        /*
        executeMysql("Log warnings about encounters with encounter date after the entry date",
                '''
                INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type)
                SELECT p.person_id as patient_id,
                       e.encounter_id as encounter_id,
                       e.encounter_date as encounter_date,
                       'encounter_date' as field_name,
                       e.encounter_date as field_value,
                       'Encounter date after entry date' as warning_type
                from hivmigration_encounters e, hivmigration_patients p
                where e.encounter_date > e.date_created
                and e.source_patient_id = p.source_patient_id;
        ''')
        */

        executeMysql("Log warnings about duplicate encounters (multiple of same type on the same day)", '''
            INSERT INTO hivmigration_data_warnings
            (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type, warning_details)
            SELECT hp.person_id,
                   he.encounter_id,
                   he.encounter_date,
                   'duplicate count',
                   count(1),
                   'Multiple encounters with same type for same patient on same day',
                   CONCAT('source encounter type: ', he.source_encounter_type)
            FROM hivmigration_encounters he
            JOIN hivmigration_patients hp on he.source_patient_id = hp.source_patient_id
            WHERE he.source_encounter_type='intake' or he.source_encounter_type='followup'
            GROUP BY he.source_patient_id, he.source_encounter_type, he.encounter_date
            HAVING count(1) > 1;
        ''')

        executeMysql("Load encounter table from staging table", '''
            insert into encounter (encounter_id, uuid, encounter_datetime, date_created, encounter_type, form_id, patient_id, creator, location_id)
            select
                e.encounter_id,
                e.encounter_uuid,
                IF(e.encounter_date IS NULL, e.date_created, e.encounter_date),
                e.date_created,
                e.encounter_type_id,
                e.form_id,
                p.person_id,
                COALESCE(hu.user_id, 1),
                COALESCE(e.location_id, 1)
            from
                hivmigration_encounters e
                    inner join
                hivmigration_patients p on e.source_patient_id = p.source_patient_id
                    left join
                hivmigration_users hu on e.source_creator_id = hu.source_user_id
            where e.encounter_type_id is not null
            ;
        ''')

        executeMysql("Log warnings for encounters with null encounter dates", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, field_name, warning_type, warning_details, flag_for_review)
            SELECT p.person_id,
                   e.encounter_id,
                   'encounter_date',
                   'Encounter date is null. `date_created` used as encounter date.',
                   CONCAT('Source encounter_type :', e.source_encounter_type),
                   TRUE as flag_for_review
            FROM hivmigration_encounters e
            JOIN hivmigration_patients p ON p.source_patient_id = e.source_patient_id
            WHERE encounter_date IS NULL;
        ''')

        executeMysql("Log warnings for patients with multiple intake encounters", '''
            INSERT INTO hivmigration_data_warnings
                (openmrs_patient_id, warning_type, warning_details, flag_for_review)
            SELECT p.person_id,
                   'Patient has multiple intake encounters',
                   CONCAT('Encounter IDs: ', GROUP_CONCAT(e.encounter_id), '. Dates: ', GROUP_CONCAT(e.encounter_date)),
                   TRUE as flag_for_review
            FROM hivmigration_encounters e
            JOIN hivmigration_patients p ON p.source_patient_id = e.source_patient_id
            WHERE e.source_encounter_type = 'intake'
            GROUP BY p.person_id
            HAVING count(distinct e.encounter_id) > 1;
        ''')

        executeMysql("Default null encounter dates to the entry date",'''
            UPDATE hivmigration_encounters
            SET encounter_date = date_created
            WHERE encounter_date IS NULL;
        ''')
    }

    void revert() {
        clearTable("encounter")
        executeMysql("DROP TABLE if exists hivmigration_encounters;")
    }
}
