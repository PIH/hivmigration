package org.pih.hivmigration.etl.sql

class AccompagnateurMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        create_tmp_obs_table()

        executeMysql("Load Accompagnateur names as observations on the most recent followup or intake form", ''' 
                
        INSERT INTO tmp_obs (
            value_text, 
            source_patient_id, 
            source_encounter_id, 
            concept_uuid)                
        SELECT 
            p.accompagnateur_name, 
            e.source_patient_id, 
            e.source_encounter_id, 
            concept_uuid_from_mapping('CIEL', '164141') as concept_uuid
        FROM hivmigration_patients p 
        INNER JOIN hivmigration_encounters e 
            on p.source_patient_id = e.source_patient_id and p.accompagnateur_name is not null
        INNER JOIN
            (SELECT source_patient_id, MAX(encounter_date) AS MaxEncounterDate
            FROM hivmigration_encounters where  source_encounter_type in ('intake', 'followup') 
            GROUP BY source_patient_id) ee 
        ON e.source_patient_id = ee.source_patient_id 
        AND e.encounter_date = ee.MaxEncounterDate 
        and e.source_encounter_type in ('intake', 'followup');
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {}
}
