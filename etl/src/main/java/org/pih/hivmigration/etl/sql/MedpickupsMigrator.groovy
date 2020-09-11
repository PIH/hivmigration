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
              dac BOOLEAN, -- dispensed in community
              months_dispensed int,
              next_dispense_date date,
              art_treatment_line VARCHAR(16), -- first_line, second_line, third_line,
              KEY `source_encounter_id_idx` (`source_encounter_id`)
            );
        ''')
        setAutoIncrement("hivmigration_dispensing", "(select max(obs_id)+1 from obs)")

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
                 dac,
                 months_dispensed,
                 next_dispense_date,
                 art_treatment_line)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            ''', '''
            select  d.encounter_id as source_encounter_id,
                    e.patient_id as source_patient_id,
                    d.dispensed_to as dispensed_to,
                    d.accompagnateur_name as accompagnateur_name,
                    case d.dac_p 
                        when 't' then 1 
                        when 'f' then 0 
                        else null
                    end dac   
                    , d.months_dispensed as months_dispensed,
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

        executeMysql("Create tmp_obs_table", ''' 
            CALL create_tmp_obs_table();
            ''')

        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_dispensing)")

        executeMysql("Load lab results as observations", ''' 
                
            SET @person_at_visit_concept_uuid = 'd0d91980-6788-4325-80d3-3bd7b54e705a';
            SET @patient_at_visit_concept_uuid = '1249F550-FF24-4F3E-A743-B37232E9E1C3';
            SET @chw_at_visit_concept_uuid = 'bf997029-a496-41a2-a7e7-7981e82d2dd0';
                        
            -- Person at visit: accompagnateur(CHW) or patient
             INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
             SELECT IF(dispensed_to='patient', @patient_at_visit_concept_uuid, @chw_at_visit_concept_uuid), 
                source_patient_id, source_encounter_id, @person_at_visit_concept_uuid
             FROM hivmigration_dispensing
             WHERE dispensed_to is not null;
             
             -- Accompagnateur name: Name of CHW
             SET @name_of_chw_concept_uuid = '164141AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
             INSERT INTO tmp_obs (value_text, source_patient_id, source_encounter_id, concept_uuid)
             SELECT accompagnateur_name, source_patient_id, source_encounter_id, @name_of_chw_concept_uuid
             FROM hivmigration_dispensing
             WHERE accompagnateur_name is not null;
            
            -- Dispensed in community: DAC
             SET @dispensing_location_concept_uuid = '6c2f54c5-e3f5-44e7-b4bb-215cfdab5e82';
             SET @community_health_location_concept_uuid = '5d4db9da-ae1e-4863-98de-cd5095c01cc8';
            
             INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
             SELECT IF(dac=true, @community_health_location_concept_uuid, null), 
                source_patient_id, source_encounter_id, @dispensing_location_concept_uuid
             FROM hivmigration_dispensing
             WHERE dac is not null;
            
             -- Treatment Line
             SET @arv_regimen_concept_uuid = '164432AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
             SET @first_line_concept_uuid = '697e9461-f2d6-4ab1-a140-48f768ce002a';
             SET @second_line_uuid = '11c0f708-6950-4e94-b080-5c76174a4947';
             SET @third_line_concept_uuid = '224e3d57-f6d1-4244-bbe2-b81a574ba7aa';
             
             INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
             SELECT CASE art_treatment_line 
                        WHEN 'first_line' THEN @first_line_concept_uuid
                        WHEN 'second_line' THEN @second_line_uuid
                        WHEN 'third_line' THEN @third_line_concept_uuid
                        end 
                        , source_patient_id
                        , source_encounter_id
                        , @arv_regimen_concept_uuid
             FROM hivmigration_dispensing
             WHERE art_treatment_line is not null;
            
            
            -- Number of months dispensed
             SET @months_dispensed = '9515cb6d-a243-454b-a77f-d762d50cba84';
             
             INSERT INTO tmp_obs (value_numeric, source_patient_id, source_encounter_id, concept_uuid)
             SELECT months_dispensed, source_patient_id, source_encounter_id, @months_dispensed
             FROM hivmigration_dispensing
             WHERE months_dispensed is not null;
             
             -- Next dispense date
             SET @next_dispense_date = '3ce94df0-26fe-102b-80cb-0017a47871b2';
             
             INSERT INTO tmp_obs (value_datetime, source_patient_id, source_encounter_id, concept_uuid)
             SELECT next_dispense_date, source_patient_id, source_encounter_id, @next_dispense_date
             FROM hivmigration_dispensing
             WHERE next_dispense_date is not null;
            
            CALL migrate_tmp_obs();
        ''')
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_dispensing_meds")
        executeMysql("drop table if exists hivmigration_dispensing")
    }
}
