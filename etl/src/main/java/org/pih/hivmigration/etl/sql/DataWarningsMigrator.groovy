package org.pih.hivmigration.etl.sql

class DataWarningsMigrator extends SqlMigrator {

    @Override
    def void migrate() {

        // First, use this migrator to add some additional data warnings that don't fit into any single migrator

        executeMysql("Add data warnings for numeric obs with invalid decimal values", '''
            INSERT INTO hivmigration_data_warnings
                        (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type, flag_for_review)
            SELECT      o.person_id, o.encounter_id, concept_name(o.concept_id, 'en'), o.value_numeric, 'Decimal obs for non-decimal numeric concept', TRUE
            FROM        obs o
            INNER JOIN  concept_numeric cn ON o.concept_id = cn.concept_id
            WHERE       cn.allow_decimal = 0
            AND         o.value_numeric != ceil(o.value_numeric)
        ''')

        executeMysql("Add data warnings for numeric obs with low out of range values", '''
            INSERT INTO hivmigration_data_warnings
                        (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type, warning_details, flag_for_review)
            SELECT      o.person_id, o.encounter_id, concept_name(o.concept_id, 'en'), o.value_numeric, 'Decimal obs lower than allowed range', concat('Low Absolute: ', cn.low_absolute), TRUE
            FROM        obs o
            INNER JOIN  concept_numeric cn ON o.concept_id = cn.concept_id
            WHERE       o.value_numeric < cn.low_absolute
        ''')

        executeMysql("Add data warnings for numeric obs with high out of range values", '''
            INSERT INTO hivmigration_data_warnings
                        (openmrs_patient_id, openmrs_encounter_id, field_name, field_value, warning_type, warning_details, flag_for_review)
            SELECT      o.person_id, o.encounter_id, concept_name(o.concept_id, 'en'), o.value_numeric, 'Decimal obs higher than allowed range', concat('High Absolute: ', cn.hi_absolute), TRUE
            FROM        obs o
            INNER JOIN  concept_numeric cn ON o.concept_id = cn.concept_id
            WHERE       o.value_numeric > cn.hi_absolute
        ''')

        executeMysql("Add data warnings for obs with null values", '''
            INSERT INTO hivmigration_data_warnings
                        (openmrs_patient_id, openmrs_encounter_id, field_name, warning_type, flag_for_review)
            SELECT      o.person_id, o.encounter_id, concept_name(o.concept_id, 'en'), 'Obs with null value', TRUE
            FROM        obs o
            INNER JOIN  (select o1.obs_id, count(o2.obs_group_id) as num_members
                         from obs o1
                         left join obs o2 on o1.obs_id = o2.obs_group_id
                         group by o1.obs_id
                        ) m on o.obs_id = m.obs_id
            WHERE       m.num_members = 0
            AND         o.value_coded is null
            AND         o.value_coded_name_id is null
            AND         o.value_datetime is null
            AND         o.value_modifier is null
            AND         o.value_complex is null
            AND         o.value_drug is null
            AND         o.value_numeric is null
            AND         o.value_text is null
        ''')

        // Next, populate additional columns in the data warnings table from source data to assist with review

        executeMysql("Insert HIV_EMR_V1 identifier in Data Warning table", '''
            UPDATE 
                hivmigration_data_warnings dw, hivmigration_patients p
            SET 
                dw.hivemr_v1_id = p.source_patient_id
            WHERE
                dw.openmrs_patient_id = p.person_id;        
        ''')

        executeMysql("Insert ZL_EMR_ID identifier in Data Warning table", '''
            
            SET @zl_emr_id = (SELECT patient_identifier_type_id FROM patient_identifier_type WHERE uuid='a541af1e-105c-40bf-b345-ba1fd6a59b85'); 
            
            UPDATE 
                hivmigration_data_warnings dw, patient_identifier pi
            SET 
                dw.zl_emr_id = pi.identifier
            WHERE
                dw.openmrs_patient_id = pi.patient_id AND
                pi.identifier_type = @zl_emr_id;        
        ''')

        executeMysql("Insert HIV Dossier identifier in Data Warning table", '''
            
            SET @hiv_dossier = (SELECT patient_identifier_type_id FROM patient_identifier_type WHERE uuid='3B954DB1-0D41-498E-A3F9-1E20CCC47323'); 
            
            UPDATE 
                hivmigration_data_warnings dw, patient_identifier pi
            SET 
                dw.hiv_dossier_id = pi.identifier
            WHERE
                dw.openmrs_patient_id = pi.patient_id AND
                pi.identifier_type = @hiv_dossier;        
        ''')

        executeMysql("Insert HIV_EMR_V1_INFANT_ID identifier in Data Warning table", '''
            UPDATE 
                hivmigration_data_warnings dw, hivmigration_infants p
            SET 
                dw.hivemr_v1_infant_id = p.source_infant_id
            WHERE
                dw.openmrs_patient_id = p.person_id;        
        ''')

        executeMysql("Insert HIV_EMR_V1_INFANT_CODE identifier in Data Warning table", '''
            UPDATE 
                hivmigration_data_warnings dw, hivmigration_infants p
            SET 
                dw.hivemr_v1_infant_code = p.infant_code
            WHERE
                dw.openmrs_patient_id = p.person_id;        
        ''')

        executeMysql("Insert HIV_EMR_encounter_id Data Warning table", '''
            UPDATE 
                hivmigration_data_warnings dw, hivmigration_encounters e
            SET 
                dw.hiv_emr_encounter_id = e.source_encounter_id
            WHERE
                dw.openmrs_encounter_id = e.encounter_id AND
                dw.openmrs_encounter_id is not null;        
        ''')

    }

    @Override
    def void revert() {

    }
}
