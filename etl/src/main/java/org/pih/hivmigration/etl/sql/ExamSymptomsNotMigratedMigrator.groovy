package org.pih.hivmigration.etl.sql

class ExamSymptomsNotMigratedMigrator extends SqlMigrator{


    @Override
    void migrate() {
        executeMysql("Create staging table for migrating HIV_EXAM_SYMPTOMS_NOT_MIGRATED", '''
            create table hivmigration_exam_symptoms_not_migrated (                                           
              source_encounter_id int,
              symptom VARCHAR(255),
              result BOOLEAN,
              symptom_date DATE,
              duration int,
              duration_unit VARCHAR(8),
              symptom_comment VARCHAR(264)                                             
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_exam_symptoms_not_migrated (
              source_encounter_id,
              symptom,
              result,
              symptom_date,
              duration,
              duration_unit,
              symptom_comment
            )
            values(?,?,?,?,?,?,?) 
            ''', '''
            select 
                s.encounter_id as source_encounter_id, 
                lower(s.symptom) as symptom, 
                case 
                    when (s.result = 't') then 1 
                    when (s.result = 'f') then 0 
                    else null 
                end as result,
                to_char(s.SYMPTOM_DATE, 'yyyy-mm-dd') as symptom_date,
                s.DURATION, 
                s.DURATION_UNIT, 
                s.SYMPTOM_COMMENT  
            from HIV_EXAM_SYMPTOMS s, HIV_ENCOUNTERS e, hiv_demographics_real d 
            where s.ENCOUNTER_ID = e.ENCOUNTER_ID and e.patient_id = d.patient_id;
        ''')
    }

    @Override
    void revert() {
        executeMysql("drop table if exists hivmigration_exam_symptoms_not_migrated")
    }
}
