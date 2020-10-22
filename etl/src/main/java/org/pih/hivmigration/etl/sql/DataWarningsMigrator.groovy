package org.pih.hivmigration.etl.sql

class DataWarningsMigrator extends SqlMigrator {


    @Override
    def void migrate() {

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
