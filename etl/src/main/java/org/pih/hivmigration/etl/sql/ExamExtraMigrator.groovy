package org.pih.hivmigration.etl.sql

class ExamExtraMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create staging table for migrating next scheduled visit date", '''
            create table hivmigration_exam_extra (                            
              source_encounter_id int,
              source_patient_id int,
              next_exam_date date            
            );
        ''')

        // note any next_exam_dates not on intake and followup form
        loadFromOracleToMySql('''
            INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, field_name, field_value, priority, note) values(?,?,?,?,?,?) 
            ''', '''
                select  e.patient_id as patient_id,
                        e.encounter_id as encounter_id,
                        'next_exam_date' as field_name,
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as field_value,
                        'WARN',
                        CONCAT('next_exam_date found on encounter of type ', e.type)
                from hiv_exam_extra x,hiv_encounters e, hiv_demographics_real d 
                where x.next_exam_date is not null and x.encounter_id = e.encounter_id and e.patient_id = d.patient_id
                    and e.type not in ('intake','followup')
            ''')


        // migrate next_exam_dates on intake and followup forms
        loadFromOracleToMySql('''
            insert into hivmigration_exam_extra (
              source_encounter_id,
              source_patient_id,
              next_exam_date 
            )
            values(?,?,?) 
            ''', '''
                select x.encounter_id as source_encounter_id,
                        e.patient_id as source_patient_id,
                        to_char(x.next_exam_date, 'yyyy-mm-dd') as next_exam_date
                from hiv_exam_extra x,hiv_encounters e, hiv_demographics_real d 
                where x.next_exam_date is not null and x.encounter_id = e.encounter_id and e.patient_id = d.patient_id
                    and e.type in ('intake','followup')
            ''')

        create_tmp_obs_table()

        executeMysql("Load next visit date as observations", ''' 
                      
            SET @next_visit_date_concept_uuid = '3ce94df0-26fe-102b-80cb-0017a47871b2';            
                        
            INSERT INTO tmp_obs (value_datetime, source_patient_id, source_encounter_id, concept_uuid)
            SELECT next_exam_date, source_patient_id, source_encounter_id, @next_visit_date_concept_uuid
            FROM hivmigration_exam_extra
            WHERE next_exam_date is not null;            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        // delete all Return Visit Dates except Next Dispensed Date (see Medpickups)
        executeMysql("Delete all Return Visit Date obs except the Next Dispense Date obs of the Dispensing encounters", '''
                delete from obs where concept_id in (select concept_id from concept where uuid='3ce94df0-26fe-102b-80cb-0017a47871b2')
                    and encounter_id not in( 
                        select e.encounter_id
                        from encounter e, encounter_type t 
                        where  e.encounter_type=t.encounter_type_id and t.uuid='cc1720c9-3e4c-4fa8-a7ec-40eeaad1958c\'
                    );          
        ''')
        executeMysql("drop table if exists hivmigration_exam_extra")
    }
}
