package org.pih.hivmigration.etl.sql

class MedpickupsMigrator extends ObsMigrator {


    @Override
    def void migrate() {

        executeMysql("Create table for mapping HIV meds to OpenMRS Drugs", '''
            create table hivmigration_openmrs_drugs (                            
              hiv_med_name VARCHAR(100) PRIMARY KEY,
              openmrs_drug_name VARCHAR(256),
              openmrs_drug_uuid CHAR(38),
              concept_uuid CHAR(38)              
            );
        ''')

        executeMysql("Add meds mappings", '''
            insert into hivmigration_openmrs_drugs(
                hiv_med_name,
                openmrs_drug_name,
                openmrs_drug_uuid,
                concept_uuid) 
            values
                ('3TC 10 mg sp','Lamivudine (3TC), Oral solution, 10mg/mL, 240mL bottle','78f96684-dfbe-11e9-8a34-2a2ae2dbcce4','3cd24e3e-26fe-102b-80cb-0017a47871b2'),
                ('ABC 20 mg','Abacavir sulfate (ABC), Oral solution, 20mg/mL, 240mL bottle','78f98308-dfbe-11e9-8a34-2a2ae2dbcce4','70057AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('ABC 300 mg','Abacavir sulfate (ABC), 300mg tablet','78f981d2-dfbe-11e9-8a34-2a2ae2dbcce4','70057AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('ABC 60 mg','Abacavir (ABC) sulfate, 60mg dispersible tablet','35c6041e-0af3-4bab-887d-9db682a02248','70057AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('ABC/3TC 120/60 mg','Abacavir (ABC) sulfate 120mg + Lamivudine (3TC) 60mg, tablet for oral suspension','a6985b15-5fd6-4bdf-93f6-62930a438464','103166AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('ABC/3TC 300/300 mg','Abacavir sulfate (ABC) 300mg + Lamivudine (3TC) 300mg tablet','f2a2c2d9-16a6-4138-9074-6fdf3307e107','103166AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('ABC/3TC 60/30 mg','Abacavir sulfate (ABC) 60mg + Lamivudine (3TC) 30mg, tablet for oral suspension','78faa9b8-dfbe-11e9-8a34-2a2ae2dbcce4','103166AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('ATV/r 300/100 mg','Atazanavir sulfate (ATV) 300mg + Ritonavir (r) 100mg tablet','78f95d38-dfbe-11e9-8a34-2a2ae2dbcce4','159809AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('AZT 10 mg sp','Zidovudine (AZT), Oral solution, 10mg/mL, 240mL bottle','78f95bc6-dfbe-11e9-8a34-2a2ae2dbcce4','3cd444be-26fe-102b-80cb-0017a47871b2'),
                ('AZT/3TC 300/150 mg','Lamivudine (3TC) 150mg + Zidovudine (AZT) 300mg tablet','78f968e6-dfbe-11e9-8a34-2a2ae2dbcce4','3cd25168-26fe-102b-80cb-0017a47871b2'),
                ('AZT/3TC 60/30 mg','Lamivudine (3TC) 30mg + Zidovudine (AZT) 60mg tablet','78f95fa4-dfbe-11e9-8a34-2a2ae2dbcce4','3cd25168-26fe-102b-80cb-0017a47871b2'),
                ('AZT/3TC/NVP 300/150/200 mg','Lamivudine (3TC) 150mg + Nevirapine (NVP) 200mg + Zidovudine (AZT) 300mg tablet','78f9739a-dfbe-11e9-8a34-2a2ae2dbcce4','3cdc4a42-26fe-102b-80cb-0017a47871b2'),
                ('AZT/3TC/NVP 60/30/50 mg','Lamivudine (3TC) 30mg + Nevirapine (NVP) 50mg + Zidovudine (AZT) 60mg dispersible tablet','78f97cfa-dfbe-11e9-8a34-2a2ae2dbcce4','3cdc4a42-26fe-102b-80cb-0017a47871b2'),
                ('Dapsone 100 mg','Dapsone, 100mg tablet','1156a9ca-14f3-4c57-9ed2-7154e82447c7','3cccd95e-26fe-102b-80cb-0017a47871b2'),
                ('DAR/ETV 300/100 mg','Darunavir (DRV) 300mg + Etravirine (ETV) 100mg, tablet','0fc3d5c1-fd39-4899-b5c0-b094e28ff359','7239d569-00ba-4a53-84de-f8754c4ca8dd'),
                ('DTG 50 mg','Dolutegravir (DTG), 50 mg tablet','78fab02a-dfbe-11e9-8a34-2a2ae2dbcce4','165085AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('DTG/3TC/TDF 50/300/300 mg','Dolutegravir (DTG) 50mg + Lamivudine (3TC) 300mg + Tenofovir disoproxil fumarate (TDF) 300mg, tablet','78faac2e-dfbe-11e9-8a34-2a2ae2dbcce4','165086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('EFV 200 mg','Efavirenz (EFV), 200mg tablet','78f97b9c-dfbe-11e9-8a34-2a2ae2dbcce4','3cd25622-26fe-102b-80cb-0017a47871b2'),
                ('EFV 600 mg','Efavirenz (EFV), 600mg tablet','78f96210-dfbe-11e9-8a34-2a2ae2dbcce4','3cd25622-26fe-102b-80cb-0017a47871b2'),
                ('ETV 100mg','Etravirine (ETV), 100mg tablet','29c9dd27-25f2-45c6-9708-93fe142a46ba','159810AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('INH 100 mg','Isoniazid (H), 100mg tablet','e47fa273-0c52-4f0f-b57b-34001a3e9677','3cd27a8a-26fe-102b-80cb-0017a47871b2'),
                ('INH 300 mg','Isoniazid (H), 300mg tablet','849218ee-901c-46b3-80f9-7c808132893b','3cd27a8a-26fe-102b-80cb-0017a47871b2'),
                ('LPV/r 100/25 mg','Lopinavir (LPV) 100mg + Ritonavir (r) 25mg tablet','78f976c4-dfbe-11e9-8a34-2a2ae2dbcce4','794AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('LPV/r 200/50 mg','Lopinavir (LPV) 200mg + Ritonavir (r) 50mg tablet','78f95e78-dfbe-11e9-8a34-2a2ae2dbcce4','794AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('LPV/r 40/10 mg','Lopinavir (LPV) 40mg + Ritonavir (r) 10mg, tablet','78faaaf8-dfbe-11e9-8a34-2a2ae2dbcce4','794AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('LPV/r 80/20 mg','Lopinavir (LPV) 80mg/mL + Ritonavir (r) 20mg/mL, Oral suspension, 160mL bottle','78faa710-dfbe-11e9-8a34-2a2ae2dbcce4','794AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('NVP 10 mg sp','Nevirapine (NVP), Oral solution, 10mg/mL, 240mL bottle','78f967ba-dfbe-11e9-8a34-2a2ae2dbcce4','3cd252f8-26fe-102b-80cb-0017a47871b2'),
                ('NVP 50 mg','Nevirapine (NVP), 50mg dispersible tablet','78f96526-dfbe-11e9-8a34-2a2ae2dbcce4','3cd252f8-26fe-102b-80cb-0017a47871b2'),
                ('NVP 200 mg','Nevirapine (NVP), 200mg tablet','78f9780e-dfbe-11e9-8a34-2a2ae2dbcce4','3cd252f8-26fe-102b-80cb-0017a47871b2'),
                ('RAL/DAR/r 400/600/100mg','Raltegravir (RAL) 400mg + Darunavir (DRV) 600mg + Ritonavir (r) 100mg, tablet','08d3817b-4e54-4825-bc4b-0512b627d567','69041e6c-aeb9-4c36-9c76-cd9c5ef4e81d'),
                ('RAL/ETV 400/100 mg','Raltegravir (RAL) 400mg + Etravirine (ETV) 100mg, tablet','c893970e-22b5-4cb9-8be2-1a9bff480235','891dcb9a-3c80-4917-a10f-07c209499413'),
                ('TDF 300 mg','Tenofovir disoproxil fumarate (TDF), 300mg tablet','78faa576-dfbe-11e9-8a34-2a2ae2dbcce4','3cd45166-26fe-102b-80cb-0017a47871b2'),
                ('TDF/3TC 300/300 mg','Tenofovir disoproxil fumarate (TDF) 300mg + Lamivudine (3TC) 300mg tablet','78f96a76-dfbe-11e9-8a34-2a2ae2dbcce4','161364AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'),
                ('TDF/3TC/EFV 300/300/600 mg','Efavirenz (EVF) 600mg + Lamivudine (3TC) 300mg + Tenofovir disoproxil fumarate (TDF) 300mg tablet','78f960da-dfbe-11e9-8a34-2a2ae2dbcce4','e43b308c-a303-4524-b4bd-a728a9f52faf'),
                ('TMS 120 mg','Cotrimoxazole (Sulfamethoxazole/Trimethoprim), 100mg/20mg tablet','160d7a20-f710-48b3-9c9b-0a1b98ab5871','3cd51772-26fe-102b-80cb-0017a47871b2'),
                ('TMS 240 mg sp','Cotrimoxazole (Sulfamethoxazole/Trimethoprim), Oral suspension, 40mg/mL + 8mg/mL, 100mL bottle','b3910fb7-2b17-44e6-8a52-8543af46c935','3cd51772-26fe-102b-80cb-0017a47871b2'),
                ('TMS 480 mg','Cotrimoxazole (Sulfamethoxazole/Trimethoprim), 400mg/80mg tablet','54972d88-156e-465e-8483-9a9e97d5898f','3cd51772-26fe-102b-80cb-0017a47871b2'),
                ('TMS 960 mg','Cotrimoxazole (Sulfamethoazxole/Trimethoprim), 800mg/160mg tablet','85153088-b868-4723-aacd-27f25f121685','3cd51772-26fe-102b-80cb-0017a47871b2');    
           ''')

        executeMysql("Create staging table", '''
            create table hivmigration_dispensing (              
              source_encounter_id int PRIMARY KEY,
              source_patient_id int,              
              dispensed_to VARCHAR(20), -- accompagnateur, patient
              accompagnateur_name VARCHAR(100),
              dac BOOLEAN, -- dispensed in community
              months_dispensed int,
              next_dispense_date date,
              art_treatment_line VARCHAR(16) -- first_line, second_line, third_line
            );
        ''')

        executeMysql("Create staging table", '''
            create table hivmigration_dispensing_meds (
              obs_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_id int,
              source_patient_id int, 
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
            from hiv_dispensing d, hiv_encounters e, hiv_demographics_real r  
            where d.encounter_id = e.encounter_id and e.patient_id = r.patient_id
            order by d.encounter_id 
        ''')

        setAutoIncrement("hivmigration_dispensing_meds", "(select max(obs_id)+1 from obs)")

        loadFromOracleToMySql('''
            insert into hivmigration_dispensing_meds
                (source_encounter_id,
                 source_patient_id,
                 source_medication_category,
                 source_product_name,
                 quantity)
            values (?, ?, ?, ?, ?)
        ''', '''
            select  m.encounter_id as source_encounter_id,     
                    e.patient_id as source_patient_id,  
                    m.med_reference as source_medication_category,       
                    p.combination_details as source_product_name,
                    m.quantity as quantity
            from hiv_dispensing_meds m, hiv_encounters e, hiv_demographics_real d, hiv_products p 
            where m.encounter_id=e.encounter_id and e.patient_id = d.patient_id and m.product_id = p.product_id 
         ''')

        create_tmp_obs_table()
        setAutoIncrement("tmp_obs", "(select max(obs_id)+1 from hivmigration_dispensing_meds)")

        executeMysql("Load meds dispensing as observations", ''' 
                
            SET @person_at_visit_concept_uuid = 'd0d91980-6788-4325-80d3-3bd7b54e705a';
            SET @patient_at_visit_concept_uuid = '1249F550-FF24-4F3E-A743-B37232E9E1C3';
            SET @chw_at_visit_concept_uuid = 'bf997029-a496-41a2-a7e7-7981e82d2dd0';
                                               
            -- Person at visit: accompagnateur(CHW) or patient
             INSERT INTO tmp_obs (value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
             SELECT IF(dispensed_to='patient', @patient_at_visit_concept_uuid, @chw_at_visit_concept_uuid), 
                source_patient_id, source_encounter_id, @person_at_visit_concept_uuid
             FROM hivmigration_dispensing
             WHERE dispensed_to is not null;
             
             -- Accompagnateur name: Name of CHW picking up the medication
             SET @name_of_chw_concept_uuid = 'c29f0c91-0128-445d-a64b-1f85498c5752';
             INSERT INTO tmp_obs (value_text, source_patient_id, source_encounter_id, concept_uuid)
             SELECT accompagnateur_name, source_patient_id, source_encounter_id, @name_of_chw_concept_uuid
             FROM hivmigration_dispensing
             WHERE accompagnateur_name is not null;
            
            -- Dispensed in community: DAC
             SET @dispensing_location_concept_uuid = '6c2f54c5-e3f5-44e7-b4bb-215cfdab5e82';
             SET @community_health_location_concept_uuid = '5d4db9da-ae1e-4863-98de-cd5095c01cc8';
            
             INSERT INTO tmp_obs (
                    value_coded_uuid, 
                    source_patient_id, 
                    source_encounter_id, 
                    concept_uuid)
             SELECT @community_health_location_concept_uuid, 
                    source_patient_id, 
                    source_encounter_id, 
                    @dispensing_location_concept_uuid
             FROM hivmigration_dispensing
             WHERE dac = true;
            
             -- Treatment Line
             SET @arv_regimen_concept_uuid = '0c709500-0cf8-4959-b244-3e9d24dcacc0';
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
                        
            -- Create Dispensing Construct
            SET @dispensing_construct_uuid = 'cef4a703-8521-4c2d-9932-d1429a57e684';
            
            INSERT INTO tmp_obs(obs_id, source_patient_id, source_encounter_id, concept_uuid) 
            SELECT obs_id, source_patient_id, source_encounter_id, @dispensing_construct_uuid
            FROM hivmigration_dispensing_meds;
            
            -- Medication category
            SET @medication_category_uuid = '3cdbc4b4-26fe-102b-80cb-0017a47871b2';
            SET @arv1_category_uuid = 'd8252da3-eac3-417e-9f84-b747f07c1c09';
            SET @arv2_category_uuid = '3ce85288-26fe-102b-80cb-0017a47871b2';
            SET @prophylaxis_category_uuid = '8b801f21-16a9-42cf-9869-9f491395765b';
                        
            INSERT INTO tmp_obs
                (obs_group_id, value_coded_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT obs_id, 
                    case source_medication_category 
                        WHEN 'arv_1' then @arv1_category_uuid
                        WHEN 'arv_2' then @arv1_category_uuid
                        ELSE @prophylaxis_category_uuid
                    end source_medication_category   
                    , source_patient_id
                    , source_encounter_id
                    , @medication_category_uuid
            FROM hivmigration_dispensing_meds
            WHERE source_medication_category IS NOT NULL;
            
            -- Medication Orders: Drug name
            SET @medication_order_uuid = '3cd9491e-26fe-102b-80cb-0017a47871b2';
            
            INSERT INTO tmp_obs
                (obs_group_id, value_coded_uuid, value_drug_uuid, source_patient_id, source_encounter_id, concept_uuid)
            SELECT d.obs_id, 
                     case when (d.source_product_name is not null) then 
                            (select h.concept_uuid from hivmigration_openmrs_drugs h where h.hiv_med_name=d.source_product_name)
                        else ifnull(d.source_product_name, '') 
                     end as 'conceptUuid',
                     case when (d.source_product_name is not null) then 
                        (select h.openmrs_drug_uuid from hivmigration_openmrs_drugs h where h.hiv_med_name=d.source_product_name)
                        else ifnull(d.source_product_name, '') 
                     end as 'drugUuid'  
                    , d.source_patient_id
                    , d.source_encounter_id
                    , @medication_order_uuid
            from hivmigration_dispensing_meds d
            where d.source_product_name is not null 
            and d.source_product_name in (select hiv_med_name from hivmigration_openmrs_drugs);
                        
            -- Quantity of medication dispensed
            SET @quantity_of_medication_dispensed_uuid = '95d216d3-8683-4582-97bd-b3ca5131e18d';
            
            INSERT INTO tmp_obs(
                      obs_group_id
                      , value_numeric
                      , source_patient_id
                      , source_encounter_id
                      , concept_uuid)
            SELECT    obs_id
                    , quantity
                    , source_patient_id
                    , source_encounter_id
                    , @quantity_of_medication_dispensed_uuid
            FROM hivmigration_dispensing_meds
            WHERE quantity IS NOT NULL;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_dispensing_meds")
        executeMysql("drop table if exists hivmigration_dispensing")
        executeMysql("drop table if exists hivmigration_openmrs_drugs")
        clearTable("obs")
    }
}
