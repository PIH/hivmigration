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
                old_relationship VARCHAR(16),
                relationship_uuid VARCHAR(28),
                age INT,
                dead_uuid VARCHAR(28),
                hiv_status_uuid VARCHAR(28)
            );
        ''')

        setAutoIncrement("hivmigration_tmp_contacts", "(select max(obs_id)+1 from obs)")

        executeMysql("Populate temp table of patient contacts", '''
            INSERT INTO hivmigration_tmp_contacts
                (source_encounter_id, first_name, last_name, old_relationship, relationship_uuid, age, dead_uuid, hiv_status_uuid)
            SELECT he.source_encounter_id,
                   substring(contact_name, 0, position(' ' IN contact_name)),
                   substring(contact_name, position(' ' IN contact_name)),
                   relationship,
                   CASE
                        WHEN cleaned_relationship IN ('fille', 'fils', 'enfant', 'enf', 'enfants', 'fiile', 'garcon', 'child', 'bellefille')
                            THEN concept_uuid_from_mapping('PIH', 'CHILD')
                        WHEN trim(lower(relationship)) = 'ex' OR cleaned_relationship IN ('partenaire', 'mari', 'concubin', 'part', 'concubine', 'femme', 'conjoint', 'epouse', 'partenairesexuel', 'epoux', 'conjointe', 'partsex', 'copine', 'fiance', 'petitami', 'copain', 'petiteamie', 'pat', 'partenairesexuelle')
                            THEN concept_uuid_from_mapping('PIH', 'PARTNER OR SPOUSE')
                        WHEN cleaned_relationship IN ('mere', 'pere', 'mére', 'pére')
                            THEN concept_uuid_from_mapping('PIH', 'GUARDIAN')
                        WHEN cleaned_relationship IN ('soeur', 'frere', 'frére')
                            THEN concept_uuid_from_mapping('PIH', 'SIBLING')
                        ELSE concept_uuid_from_mapping('PIH', 'OTHER RELATIVE')
                        END,
                    IFNULL(age, TIMESTAMPDIFF(YEAR, hc.birth_date, he.encounter_date)),
                   IF(deceased_p = 't', concept_uuid_from_mapping('PIH', 'DEATH'), NULL),
                   CASE hiv_status
                        WHEN 'not_tested' THEN concept_uuid_from_mapping('PIH', 'NO TEST')
                        WHEN 'unknown' THEN concept_uuid_from_mapping('PIH', 'unknown')
                        WHEN 'positive' THEN concept_uuid_from_mapping('PIH', 'POSITIVE')
                        WHEN 'negative' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                        WHEN 'pos' THEN concept_uuid_from_mapping('PIH', 'POSITIVE')
                        WHEN 'neg' THEN concept_uuid_from_mapping('PIH', 'NEGATIVE')
                        END
            FROM (SELECT TRIM(LEADING 'ex' FROM replace(replace(replace(replace(replace(replace(trim(lower(relationship)), ' ', ''), '-', ''), '.', ''), 'son', ''), 'sa', ''), 'ses', '')) as cleaned_relationship, *
                  FROM hivmigration_contacts) hc
            JOIN hivmigration_encounters he
                ON hc.source_patient_id = he.source_patient_id
                AND he.source_encounter_type = 'intake';
        ''')

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
                   concept_uuid_from_mapping('PIH', 'RELATIONSHIP OF RELATIVE TO PATIENT'),
                   relationship_uuid
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('CIEL', '163533'),
                   dead_uuid
            FROM hivmigration_tmp_contacts;
            
            INSERT INTO tmp_obs
                (obs_group_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT obs_group_id,
                   source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'RESULT OF HIV TEST'),
                   hiv_status_uuid
            FROM hivmigration_tmp_contacts;
        ''')
    }

    @Override
    def void revert() {

    }
}
