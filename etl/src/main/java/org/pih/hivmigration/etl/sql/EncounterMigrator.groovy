package org.pih.hivmigration.etl.sql

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
              creator int,
              encounter_uuid char(38),
              encounter_date date,
              performed_by varchar(100),
              date_created datetime,
              comments varchar(4000),
              note_title varchar(100),
              response_to int,
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

        loadFromOracleToMySql(
                '''
                insert into hivmigration_encounters(source_encounter_id,source_patient_id,source_encounter_type,source_creator_id,encounter_date,date_created,comments,performed_by,source_location_id,note_title,response_to)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    e.response_to
                from
                    hiv_encounters e

            '''
        )
        executeMysql('''
            update hivmigration_encounters SET encounter_uuid = uuid();
        ''')

        executeMysql("Fill encounter types column", '''
            SET @encounter_type_intake = (select encounter_type_id from encounter_type where uuid = 'c31d306a-40c4-11e7-a919-92ebcb67fe33');
            SET @encounter_type_followup = (select encounter_type_id from encounter_type where uuid = 'c31d3312-40c4-11e7-a919-92ebcb67fe33');
            SET @encounter_type_lab_results = (select encounter_type_id from encounter_type where uuid = '4d77916a-0620-11e5-a6c0-1697f925ec7b');
            
            UPDATE hivmigration_encounters SET encounter_type_id = CASE
                WHEN source_encounter_type = "intake" THEN @encounter_type_intake
                WHEN source_encounter_type = "followup" THEN @encounter_type_followup
                WHEN source_encounter_type = "lab_result" THEN @encounter_type_lab_results
                WHEN source_encounter_type = "anlap_lab_result" THEN @encounter_type_lab_results
                END
        ''')
        // TODO: Handle source_encounter_type "anlap_vital_signs", "patient_contact", "food_study", "regime", "note", "accompagnateur" (https://pihemr.atlassian.net/browse/UHM-3244)

//      TODO: The old code from Pentaho about hivmigration_health_center. Add back in or delete once that table is migrated.
//            update hivmigration_encounters e, hivmigration_health_center hc
//                set e.location_id = hc.openmrs_id
//                where e.source_location_id = hc.hiv_emr_id;

        executeMysql("Load encounter table from staging table", '''
            insert into encounter (encounter_id, uuid, encounter_datetime, date_created, encounter_type, patient_id, creator, location_id)
            select 
                e.encounter_id,
                e.encounter_uuid,
                e.encounter_date,
                e.date_created,
                e.encounter_type_id,
                p.person_id,
                COALESCE(hu.user_id, 1),
                COALESCE(e.location_id, 1)
            from 
                hivmigration_encounters e 
            inner join 
                hivmigration_patients p on e.source_patient_id = p.source_patient_id 
            left join
              hivmigration_users hu on e.source_creator_id = hu.source_user_id
            where e.encounter_type_id is not null  # TODO: still need to migrate 'note' and 'regime' https://pihemr.atlassian.net/browse/UHM-3244
              and e.encounter_date is not null   # TODO: figure out what to do with these https://pihemr.atlassian.net/browse/UHM-3237
            ;
        ''')
    }

    void revert() {
        clearTable("encounter")
        executeMysql("DROP TABLE if exists hivmigration_encounters;")
    }
}
