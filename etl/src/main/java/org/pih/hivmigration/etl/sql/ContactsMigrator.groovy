package org.pih.hivmigration.etl.sql

/**
 * Migrates Intake form section 11, "Contacts" to the HIV Intake form.
 *
 * Data comes from HIV_CONTACTS / hivmigration_contacts, which is imported by StagingTablesMigrator.
 */
class ContactsMigrator extends ObsMigrator {
    @Override
    def void migrate() {
        executeMysql("Create temporary table for contact person groups", '''
            CREATE TABLE hivmigration_tmp_contacts (
                obs_group_id INT PRIMARY KEY AUTO_INCREMENT,
                source_encounter_id INT,
                first_name VARCHAR(64),
                last_name VARCHAR(64),
                old_relationship VARCHAR(76),
                relationship_uuid VARCHAR(38),
                age INT,
                death_uuid VARCHAR(38),
                hiv_status_uuid VARCHAR(38)
            );
        ''')

        setAutoIncrement("hivmigration_tmp_contacts", "(select max(obs_id)+1 from obs)")

        executeMysql("Populate temp table of patient contacts", '''
            INSERT INTO hivmigration_tmp_contacts
                (source_encounter_id, first_name, last_name, old_relationship, relationship_uuid, age, death_uuid, hiv_status_uuid)
            SELECT he.source_encounter_id,
                   left(trim(contact_name), char_length(trim(contact_name)) - LOCATE(' ', REVERSE(trim(contact_name)))+1) as first,
                   substring_index(trim(contact_name), ' ', -1) as last,
                   relationship,
                   CASE
                        WHEN relationship REGEXP 'fil|enf|fiil|garcon|child\'
                            THEN concept_uuid_from_mapping('PIH', 'GUARDIAN RELATIONSHIP TO CHILD')  -- replace with PIH:CHILD once the latter is available
                        WHEN relationship REGEXP 'ex|part|mari|conc|femme|conjoint|epou|cop|fiance|petit|pat|husband\'
                            THEN concept_uuid_from_mapping('PIH', 'PARTNER OR SPOUSE')
                        WHEN relationship REGEXP 'mere|pere|mére|pére\'
                            THEN concept_uuid_from_mapping('PIH', 'GUARDIAN')
                        WHEN relationship REGEXP 'soeur|frere|frére\'
                            THEN concept_uuid_from_mapping('PIH', 'SIBLING')
                        WHEN relationship IS NULL THEN NULL
                        ELSE concept_uuid_from_mapping('PIH', 'OTHER RELATIVE')
                        END as relationship_uuid,
                   TIMESTAMPDIFF(YEAR, hc.birth_date, he.encounter_date) as age,
                   IF(deceased_p = 't', concept_uuid_from_mapping('PIH', 'DEATH'), NULL) as death_uuid,
                   CASE hiv_status
                        WHEN 'not_tested' THEN concept_uuid_from_mapping('PIH', 'NO TEST')
                        WHEN 'unknown' THEN concept_uuid_from_mapping('PIH', 'unknown')
                        WHEN 'positive' THEN concept_uuid_from_mapping('PIH', 'POSITIVE')
                        WHEN 'negative' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                        WHEN 'pos' THEN concept_uuid_from_mapping('PIH', 'POSITIVE')
                        WHEN 'neg' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                        END as hiv_status_uuid
            FROM hivmigration_contacts hc
            JOIN hivmigration_encounters he
                ON hc.source_patient_id = he.source_patient_id
                AND he.source_encounter_type = 'intake';
        ''')

        create_tmp_obs_table()

        executeMysql("Create tmp_obs from contacts table", '''
            INSERT INTO tmp_obs
                (obs_id, source_encounter_id, concept_uuid)
            SELECT obs_group_id, source_encounter_id, concept_uuid_from_mapping('PIH', 'Contact construct')
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_text)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'FIRST NAME'),
                   first_name
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_text)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'LAST NAME'),
                   last_name
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_numeric)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'AGE'),
                   age
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
            (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '164352'),  -- Relationship to patient
                   relationship_uuid
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '163533'),  -- Health status of contact
                   death_uuid
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'RESULT OF HIV TEST'),
                   hiv_status_uuid
            FROM hivmigration_tmp_contacts;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        clearTable("obs")
        executeMysql("DROP TABLE IF EXISTS hivmigration_tmp_contacts;")
    }
}
