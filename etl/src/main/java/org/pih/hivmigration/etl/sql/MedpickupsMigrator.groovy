package org.pih.hivmigration.etl.sql

class MedpickupsMigrator extends SqlMigrator{


    @Override
    def void migrate() {
        executeMysql("Create staging table", '''
            create table hivmigration_dispensing (
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              source_patient_id int,              
              dispensed_to VARCHAR(20), -- accompagnateur, patient
              accompagnateur_name VARCHAR(100),
              months_dispensed int,
              next_dispense_date date,
              art_treatment_line VARCHAR(16), -- first_line, second_line, third_line,
              KEY `source_encounter_id_idx` (`source_encounter_id`)
            );
        ''')

        executeMysql("Create staging table", '''
            create table hivmigration_dispensing_meds (
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              source_medication_category VARCHAR(16), -- arv_1, arv_2, inh_1, tms_1             
              source_product_name VARCHAR(100), -- HIV_PRODUCTS.COMBINATION_DETAILS              
              quantity int
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_dispensing
                (source_encounter_id,
                 source_patient_id,
                 dispensed_to,
                 accompagnateur_name,
                 months_dispensed,
                 next_dispense_date,
                 art_treatment_line)
            values (?, ?, ?, ?, ?, ?, ?)
            ''', '''
            select  d.encounter_id as source_encounter_id,
                    e.patient_id as source_patient_id,
                    d.dispensed_to as dispensed_to,
                    d.accompagnateur_name as accompagnateur_name,
                    d.months_dispensed as months_dispensed,
                    to_char(d.next_dispense_date, 'yyyy-mm-dd') as next_dispense_date,
                    d.art_treatment_line as art_treatment_line
            from hiv_dispensing d, hiv_encounters e 
            where d.encounter_id = e.encounter_id 
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_dispensing_meds
                (source_encounter_id,
                 source_medication_category,
                 source_product_name,
                 quantity)
            values (?, ?, ?, ?)
        ''', '''
            select  m.encounter_id as source_encounter_id,       
                    m.med_reference as source_medication_category,       
                    p.combination_details as source_product_name,
                    m.quantity as quantity
            from hiv_dispensing_meds m, hiv_products p 
            where m.product_id = p.product_id 
         ''')
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_dispensing_meds")
        executeMysql("drop table if exists hivmigration_dispensing")
    }
}
