package org.pih.hivmigration.etl.sql

class EncounterMigrator extends SqlMigrator {

    void migrate() {
        executeMysql('''
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
            update hivmigration_encounters set source_location_id = 0 where source_location_id is null;
            update hivmigration_encounters e, hivmigration_health_center hc
                set e.location_id = hc.openmrs_id 
                where e.source_location_id = hc.hiv_emr_id;
        ''')

        // This should be doing an actual insert or something
        executeMysql('''
            insert into encounter (encounter_id, uuid, encounter_date, date_created, encounter_type_id, patient_id, creator, location_id)
            select 
                e.encounter_id,
                e.encounter_uuid,
                e.encounter_date,
                e.date_created,
                et.encounter_type_id,
                p.person_id,
                u.user_id,
                e.location_id,
            from 
                hivmigration_encounters e 
            inner join 
                hivmigration_patients p on e.source_patient_id = p.source_patient_id 
            inner join
                hivmigration_encounter_type et on et.encounter_type = e.source_encounter_type
            left join
              hivmigration_users hu on e.source_creator_id = hu.source_user_id
            left join
              users u on u.uuid = hu.user_uuid
            
            # TODO: we should not just ignore these, but figure out what to do with them
            where e.encounter_date is not null;

        ''')
    }

    void revert() {
        clearTable("obs", true);
        clearTable("encounter");
        executeMysql("DROP TABLE hivmigration_encounters;");
    }
}