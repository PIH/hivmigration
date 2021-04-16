package org.pih.hivmigration.etl.sql

class RegimenMigrator extends SqlMigrator {

    @Override
    void migrate() {

        createStagingTable()
        setAutoIncrements()
        loadStagingTableWithRawDataFromHivEmr()
        cleanInputDataAndRaiseDataWarnings()

        loadStagingTableWithNewOrders()
        loadStagingTableWithDiscontinueOrders()

        populateStagingTableWithDerivedValues()
        updateEncounterDatesOnExistingDrugOrderEncounters()
        createNewEncountersAndAssociateBackToStagingTable()

        populateDrugAndConceptIdsForEachProductId()
        populateOrderReasonForNewOrders()
        populateOrderReasonForDiscontinueOrders()
        populateDosingInstructions()

        createOrders()
        createDrugOrders()

        removeRegimenEncountersThatHaveNoAssociatedDrugOrders()

        validateResults()
    }

    void createStagingTable() {
        executeMysql("Create staging tables", '''

            create table hivmigration_regimes (
            
              -- Source data
              source_regime_id int primary key,
              source_encounter_opened_by int,
              source_encounter_closed_by int,
              source_patient_id int,
              source_product_id int,
              source_product_name varchar(100), -- this is the drug/dose/unit associated with the product_id in the source table
              source_start_date date,
              source_end_date date,
              source_close_date date,
              source_close_date_estimated boolean,
              source_reason_for_closure varchar(20),
              source_product_type varchar(20), -- this comes from the hiv_products table joined on hiv_regimes
              source_regime_type varchar(50), -- from regime_type
              source_ddd double, -- daily dose
              source_dwd double, -- weekly dose
              source_prn boolean, -- as needed
              source_morning_dose double,
              source_noon_dose double,
              source_evening_dose double,
              source_night_dose double,
              
              -- Cleaned source data leading to data warnings
              cleaned_end_date date,
              cleaned_close_date date,
              
              KEY `source_regime_id_idx` (`source_regime_id`),
              KEY `source_patient_id_idx` (`source_patient_id`),
              KEY `source_encounter_opened_by_idx` (`source_encounter_opened_by`),
              KEY `source_encounter_closed_by_idx` (`source_encounter_closed_by`),
              KEY `source_product_id_idx` (`source_product_id`),
              KEY `source_product_name_idx` (`source_product_name`)
            );

            create table hivmigration_drug_orders (
            
              order_id int PRIMARY KEY AUTO_INCREMENT,
              source_regime_id int,
              
              -- Derived data
              order_uuid char(38),
              order_action varchar(15), -- For a given source_regime_id, there will be one row with NEW, and 0-1 with DISCONTINUE
              encounter_id int,
              patient_id int,
              previous_order_id int,
              drug_id int, -- This should be mapped in based on the source_product_id
              drug_non_coded varchar(100),
              concept_id int, -- This can be auto-populated from the drug_id or mapped in based on source_product_id
              date_activated date, -- This ultimately needs to be populated to match the encounter_date that gets created
              urgency varchar(20),
              start_date date,
              date_stopped date, -- This comes from close_date as long as it is not in the future
              auto_expire_date date, -- this comes from end_date or from close date if close date is in the future
              order_reason int, -- This should be mapped based on what is in the source_reason_for_closure, source regime_type, and source_product_type
              order_reason_non_coded varchar(100),
              as_needed boolean,
              dosing_instructions varchar(100),
              
              KEY `source_regime_id_idx` (`source_regime_id`),
              KEY `order_action_idx` (`order_action`),
              KEY `patient_id_idx` (`patient_id`),
              KEY `encounter_id_idx` (`encounter_id`),
              KEY `date_activated_idx` (`date_activated`)
            );
            
        ''')
    }

    void setAutoIncrements() {
        setAutoIncrement("hivmigration_drug_orders", "(select max(order_id)+1 from orders)")
    }

    void loadStagingTableWithRawDataFromHivEmr() {
        loadFromOracleToMySql('''
            insert into hivmigration_regimes (
              source_regime_id,
              source_encounter_opened_by,
              source_encounter_closed_by,
              source_patient_id,
              source_product_id,
              source_product_name,
              source_start_date,
              source_end_date,
              source_close_date,
              source_close_date_estimated,
              source_reason_for_closure,
              source_product_type,
              source_regime_type,
              source_ddd,
              source_dwd,
              source_prn,
              source_morning_dose,
              source_noon_dose,
              source_evening_dose,
              source_night_dose
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', '''
            select
                   r.regime_id as source_regime_id,
                   r.encounter_opened_by as source_encounter_opened_by,
                   r.encounter_closed_by as source_encounter_closed_by,
                   r.patient_id as source_patient_id,
                   r.product_id as source_product_id,
                   (trim(p.generic_fr) || ' - ' || p.strength_dose || ' ' || p.strength_unit || ' ' || p.dosage_form) as source_product_name,
                   r.start_date as source_start_date,
                   r.end_date as source_end_date,
                   r.close_date as source_close_date,
                   decode(r.close_date_estimated_p, 't', 1, 0) as source_close_date_estimated,
                   r.reason_for_closure as source_reason_for_closure,
                   p.prod_type_flag as source_product_type,
                   r.regime_type as source_regime_type,
                   r.ddd as source_ddd,
                   r.dwd as source_dwd,
                   decode(r.prn, 't', 1, 0) as source_prn,
                   r.morning_dose as source_morning_dose,
                   r.noon_dose as source_noon_dose,
                   r.evening_dose as source_evening_dose,
                   r.night_dose as source_night_dose
            from
                   hiv_regimes_real r, hiv_products p, hiv_demographics_real d
            where
                   r.product_id = p.product_id
            and    r.patient_id = d.patient_id
            ;
        ''')
    }

    void cleanInputDataAndRaiseDataWarnings() {
        executeMysql("Clean regimens that have close date prior to start date", '''           
            insert  into hivmigration_data_warnings (
                        openmrs_patient_id, hiv_emr_encounter_id, field_name, field_value, warning_type, warning_details, flag_for_review)
            select      p.person_id, o.source_encounter_opened_by, 'close_date', date_format(o.source_close_date, '%Y-%m-%d'),
                        'Drug Order Close Date is before Start Date.  Setting to equal Start Date.',
                        concat(
                            'Drug: ', o.source_product_name, 
                            '; Start Date: ', date_format(o.source_start_date, '%Y-%m-%d'), 
                            '; Close Date: ', date_format(o.source_close_date, '%Y-%m-%d')
                        ),
                        1
            from        hivmigration_regimes o
            inner join  hivmigration_patients p on p.source_patient_id = o.source_patient_id
            where       o.source_close_date < o.source_start_date
            ;
            
            update  hivmigration_regimes set cleaned_close_date = source_close_date;
            update  hivmigration_regimes set cleaned_close_date = source_start_date where source_close_date < source_start_date;
        ''')

        executeMysql("Clean regimens that close in the future", '''
            insert  into hivmigration_data_warnings (
                        openmrs_patient_id, hiv_emr_encounter_id, field_name, field_value, warning_type, warning_details, flag_for_review)
            select      p.person_id, o.source_encounter_opened_by, 'close_date', date_format(source_close_date, '%Y-%m-%d'),
                        'Drug Order Close Date is in the future.  Reclassifying this as Auto-Expire Date.',
                        concat(
                            'Drug: ', source_product_name, 
                            '; Start Date: ', date_format(o.source_start_date, '%Y-%m-%d'), 
                            '; Close Date: ', date_format(o.source_close_date, '%Y-%m-%d')
                        ),
                        1
            from        hivmigration_regimes o
            inner join  hivmigration_patients p on p.source_patient_id = o.source_patient_id
            where       o.source_close_date > now()
            ;
            
            update hivmigration_regimes set cleaned_end_date = source_end_date;
            update hivmigration_regimes set cleaned_end_date = source_close_date, cleaned_close_date = null where source_close_date > now();
        ''')
    }

    void loadStagingTableWithNewOrders() {
        executeMysql('''
            insert into hivmigration_drug_orders (
                source_regime_id,
                order_uuid,
                order_action,
                encounter_id,
                patient_id,
                start_date,
                auto_expire_date,
                date_stopped
            )
            select
                r.source_regime_id,
                uuid() as order_uuid,
                'NEW' as order_action,
                e.encounter_id,
                p.person_id,
                r.source_start_date,
                r.cleaned_end_date,
                r.cleaned_close_date
            from
                hivmigration_regimes r
            left join
                hivmigration_encounters e on e.source_encounter_id = r.source_encounter_opened_by
            inner join
                hivmigration_patients p on p.source_patient_id = r.source_patient_id
            ;
        ''')
    }

    void loadStagingTableWithDiscontinueOrders() {
        executeMysql('''
            insert into hivmigration_drug_orders (
                source_regime_id,
                order_uuid,
                order_action,
                previous_order_id,
                encounter_id,
                patient_id,
                start_date,
                auto_expire_date
            )
            select
                d.source_regime_id,
                uuid() as order_uuid,
                'DISCONTINUE' as order_action,
                d.order_id as previous_order_id,
                e.encounter_id,
                d.patient_id,
                d.date_stopped,
                d.date_stopped
            from
                hivmigration_drug_orders d
            inner join
                hivmigration_regimes r on r.source_regime_id = d.source_regime_id
            left join
                hivmigration_encounters e on e.source_encounter_id = r.source_encounter_closed_by
            where
                d.date_stopped is not null
            ;
        ''')
    }

    void populateStagingTableWithDerivedValues() {

        executeMysql("Populate date activated on orders associated with encounters", '''
            update  hivmigration_drug_orders o, 
                    (select encounter_id, min(start_date) as first_start_date from hivmigration_drug_orders group by encounter_id) a
            set     o.date_activated = a.first_start_date
            where   o.encounter_id is not null 
            and     o.encounter_id = a.encounter_id
        ''')

        executeMysql("Populate date activated on orders not associated with encounters", '''
            update  hivmigration_drug_orders o
            set     o.date_activated = o.start_date
            where   o.encounter_id is null 
        ''')

        executeMysql("Set urgency to ROUTINE or ON_SCHEDULED_DATE as appropriate", '''
            update  hivmigration_drug_orders o
            set     o.urgency = if(o.date_activated = o.start_date, 'ROUTINE', 'ON_SCHEDULED_DATE')
        ''')

        executeMysql("Match encounterless orders with existing encounters where possible", '''
            update  hivmigration_drug_orders o,
                    (   select patient_id, encounter_id, max(date_activated) as date_activated
                        from hivmigration_drug_orders
                        where encounter_id is not null
                        group by patient_id, encounter_id
                    ) x
            set     o.encounter_id = x.encounter_id
            where   o.patient_id = x.patient_id
            and     o.date_activated = x.date_activated
            and     o.encounter_id is null
        ''')
    }

    void updateEncounterDatesOnExistingDrugOrderEncounters() {
        executeMysql("Update encounter dates to reflect date activated of associated orders", '''
            update      encounter e 
            inner join  hivmigration_drug_orders o on e.encounter_id = o.encounter_id
            inner join  encounter_type t on e.encounter_type = t.encounter_type_id
            set         e.encounter_datetime = o.date_activated
            where       t.uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002'
        ''')
    }

    void createNewEncountersAndAssociateBackToStagingTable() {
        executeMysql("Create new encounters for orders that cannot be associated with existing encounters", '''
            SET @encounter_type_id = (select encounter_type_id from encounter_type where uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002');
            SET @form_id = (select form_id from form where uuid = '96482a6e-5b62-11eb-8f5a-0242ac110002');
            insert into encounter 
                (uuid, patient_id, encounter_datetime, encounter_type, form_id, location_id, date_created, creator)
            select
                uuid(), x.patient_id, x.date_activated,  @encounter_type_id, @form_id, 1, now(), 1
            from
                (   select patient_id, date_activated 
                    from hivmigration_drug_orders 
                    where encounter_id is null
                    group by patient_id, date_activated
                 ) x
            ;
        ''')

        executeMysql("Associate newly created encounters back with the encounter-less orders", '''
            update      hivmigration_drug_orders o 
            inner join  encounter e on o.patient_id = e.patient_id and o.date_activated = e.encounter_datetime
            inner join  encounter_type t on e.encounter_type = t.encounter_type_id and t.uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002'
            set         o.encounter_id = e.encounter_id
            where       o.encounter_id is null
        ''')
    }

    void populateDrugAndConceptIdsForEachProductId() {
        executeMysql("Populate drug ids that map to each product id", '''

            DROP PROCEDURE IF EXISTS populate_drug;
            DELIMITER $$ ;
            CREATE PROCEDURE populate_drug( _product_name varchar(100), _concept_uuid char(36), _drug_uuid char(36))
            BEGIN
                SET @drug_id = null;
                SET @concept_id = null;
                SET @drug_non_coded = null;
                IF (_drug_uuid is null or _drug_uuid = '') THEN
                    SET @concept_id = (select concept_id from concept where uuid = _concept_uuid);
                    SET @drug_non_coded = _product_name;
                ELSE
                    SET @drug_id = (select drug_id from drug where uuid = _drug_uuid);
                    SET @concept_id = (select concept_id from drug where uuid = _drug_uuid);
                END IF;
                
                UPDATE      hivmigration_drug_orders d
                INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
                SET         d.drug_id = @drug_id, 
                            d.concept_id = @concept_id, 
                            d.drug_non_coded = @drug_non_coded 
                WHERE       r.source_product_name = _product_name;  
                
                IF (_concept_uuid = '3cee7fb4-26fe-102b-80cb-0017a47871b2') THEN
                    insert  into hivmigration_data_warnings (
                        openmrs_patient_id, field_name, field_value, warning_type, warning_details
                    )
                    select  p.person_id, 'concept', _concept_uuid, 'Missing concept mapping for drug',
                            concat('Drug: ', _product_name, ' is mapped to other non-coded concept')
                    from        hivmigration_regimes r
                    inner join  hivmigration_patients p on p.source_patient_id = r.source_patient_id
                    where       r.source_product_name = _product_name
                    ;
                END IF;
            END $$
            DELIMITER ;

            CALL populate_drug('Abacavir 20 mg/ml - 20 milligram Other', '', '78f98308-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Abacavir sulfate (ABC), Oral solution, 20mg/mL, 240mL bottle
            CALL populate_drug('Abacavir 300 mg - 300 milligram Tablet', '', '78f981d2-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Abacavir sulfate (ABC), 300mg tablet
            CALL populate_drug('Abacavir 60 mg - 60 milligram Tablet', '', '35c6041e-0af3-4bab-887d-9db682a02248'); -- Abacavir (ABC) sulfate, 60mg dispersible tablet
            CALL populate_drug('ABC/3TC 120/60 mg - 180 milligram Tablet', '', 'a6985b15-5fd6-4bdf-93f6-62930a438464'); -- Abacavir (ABC) sulfate 120mg + Lamivudine (3TC) 60mg, tablet for oral suspension
            CALL populate_drug('ABC/3TC 300/300 mg - 600 milligram Capsule', '', 'f2a2c2d9-16a6-4138-9074-6fdf3307e107'); -- Abacavir sulfate (ABC) 300mg + Lamivudine (3TC) 300mg tablet
            CALL populate_drug('ABC/3TC 60/30 mg - 90 milligram Capsule', '', '78faa9b8-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Abacavir sulfate (ABC) 60mg + Lamivudine (3TC) 30mg, tablet for oral suspension
            CALL populate_drug('Acetylsalicylic acid - 500 milligram Tablet', '3cccd35a-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Acide Acétylsalycilique - 100 milligram Tablet', '', '8d5941e8-5f86-4289-a6ba-122320814bd5'); -- Acetylsalicylic acid, 100mg tablet
            CALL populate_drug('Acyclovir - 200 milligram Tablet', '', '0962492b-295e-4f61-8d7b-717a61f43997'); -- Aciclovir, 200mg tablet
            CALL populate_drug('Acyclovir - 400 milligram Tablet', '3cd3d4de-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Albendazole - 400 milligram Tablet', '', '8112cd30-ae2a-11e4-ab27-0800200c9a66'); -- Albendazole, 400mg chewable tablet
            CALL populate_drug('Amoxycillin - 500 milligram Tablet', '', '1491ae88-f38c-4a8c-94d5-f8385eb3b9d9'); -- Amoxicillin, 500mg tablet
            CALL populate_drug('Amprenavir - 150 milligram Tablet', '71193AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Atazanavir - 300 milligram Capsule', '71647AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Atazanavir/Ritonavir 300/100 mg - 400 milligram Tablet', '', '78f95d38-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Atazanavir sulfate (ATV) 300mg + Ritonavir (r) 100mg tablet
            CALL populate_drug('Atenolol - 50 milligram Tablet', '', '2f8d7a99-d4ec-4ad7-b898-1c953cb332fd'); -- Atenolol, 50mg tablet
            CALL populate_drug('AZT+3TC 300/150 mg - 450 milligram Tablet', '', '78f968e6-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine (3TC) 150mg + Zidovudine (AZT) 300mg tablet
            CALL populate_drug('AZT+3TC 60/30 mg - 90 milligram Tablet', '', '78f95fa4-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine (3TC) 30mg + Zidovudine (AZT) 60mg tablet
            CALL populate_drug('AZT+3TC+NVP 300/150/200 mg - 650 milligram Tablet', '', '78f9739a-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine (3TC) 150mg + Nevirapine (NVP) 200mg + Zidovudine (AZT) 300mg tablet
            CALL populate_drug('AZT+3TC+NVP 60/30/50 mg - 140 milligram Tablet', '', '78f97cfa-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine (3TC) 30mg + Nevirapine (NVP) 50mg + Zidovudine (AZT) 60mg dispersible tablet
            CALL populate_drug('B- Complex - 16 milligram Tablet', '3ccf6e62-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Benzyl PNC 5,000,000IU - 1 gram Other', '', '2896a1ad-f576-4073-875e-835d40d70548'); -- Benzylpenicillin (Penicillin G), Powder for solution for injection, 5 MIU vial
            CALL populate_drug('Butylscopolamine - 10 milligram Tablet', '3cd0fc3c-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Capreomycin - 1 unspecified Other', '3cda1f60-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Capreomycine - 1 gram Ampulle', '3cda1f60-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Captopril - 50 milligram Tablet', '4add1344-ec52-41e3-b5b9-267f19d99aa2', '');
            CALL populate_drug('Carbamazépine - 200 milligram Tablet', '', 'e371d811-d32c-4f6e-8493-2fa667b7b44c'); -- Carbamazepine, 200mg film coated tablet
            CALL populate_drug('Cephalexine - 500 milligram Tablet', '', '4c908591-3adf-4601-b61c-4faddffbee56'); -- Cefalexin, 500mg capsule
            CALL populate_drug('Chloramphenicol Gttes Oph. 0.5% - 10 ml Other', '3ccefbb2-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Chloroquine - 150 milligram Tablet', '', 'a0b5fc86-543f-4162-93a7-8936a565a172'); -- Chloroquine, 150mg base (242mg phosphate) tablet
            CALL populate_drug('Cimetidine - 200 milligram Tablet', '', 'ef96f590-2cc3-469e-9b82-072fef563b9e'); -- Cimetidine, 200mg tablet
            CALL populate_drug('Ciprofloxacin - 500 milligram Tablet', '', '2a885fd0-f8c9-43fb-883b-fe25d5769338'); -- Ciprofloxacin, 500mg film coated tablet
            CALL populate_drug('Clofazimine - 50 milligram Tablet', '3cda20dc-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Cotrimoxazol (400/80mg) - 1 tab Tablet', '', '54972d88-156e-465e-8483-9a9e97d5898f'); -- Cotrimoxazole (Sulfamethoxazole/Trimethoprim), 400mg/80mg tablet
            CALL populate_drug('Cotrimoxazol (800/160mg) - 1 tab Tablet', '', '85153088-b868-4723-aacd-27f25f121685'); -- Cotrimoxazole (Sulfamethoazxole/Trimethoprim), 800mg/160mg tablet
            CALL populate_drug('Cotrimoxazol TMP/ SMX - 120 milligram Tablet', '', '160d7a20-f710-48b3-9c9b-0a1b98ab5871'); -- Cotrimoxazole (Sulfamethoxazole/Trimethoprim), 100mg/20mg tablet
            CALL populate_drug('Cotrimoxazole (200/40mg/ 5cc) - 1 item Bottle', '3cd51772-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Cycloserine - 1 unspecified Other', '3cda2262-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Cycloserine - 250 milligram Tablet', '', '78fab188-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Cycloserine, 250mg capsule
            CALL populate_drug('D4T+3TC+NVP ( Triomune-30) - 380 milligram Tablet', '', '78f97e3a-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine (3TC) 150mg + Stavudine (d4T) 30mg + Nevirapine (NVP) 200mg tablet
            CALL populate_drug('Dapsone - 100 milligram Tablet', '', '1156a9ca-14f3-4c57-9ed2-7154e82447c7'); -- Dapsone, 100mg tablet
            CALL populate_drug('Diazepam - 10 milligram Ampulle', '3ccedb64-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Didanosine - 200 milligram Tablet', '3cd44248-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Digoxine - .25 milligram Tablet', '', 'e44e484e-90ec-41ca-aec1-e3190d7be626'); -- Digoxin, 250 microgram tablet
            CALL populate_drug('Dolutegravir - 50 milligram Tablet', '', '78fab02a-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Dolutegravir (DTG), 50 mg tablet
            CALL populate_drug('Doxycycline - 100 milligram Tablet', '', '8aad2a23-2977-4b5b-a30a-4a9142ce774b'); -- Doxycycline, 100mg tablet
            CALL populate_drug('DRVr 300mg - 300 milligram Tablet', '162796AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('DTG/3TC/TDF 50/300/300 mg - 650 milligram Tablet', '', '78faac2e-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Dolutegravir (DTG) 50mg + Lamivudine (3TC) 300mg + Tenofovir disoproxil fumarate (TDF) 300mg, tablet
            CALL populate_drug('Efavirenz - 600 milligram Tablet', '', '78f96210-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Efavirenz (EFV), 600mg tablet
            CALL populate_drug('Efavirenz - 200 milligram Tablet', '', '78f97b9c-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Efavirenz (EFV), 200mg tablet
            CALL populate_drug('Efavirenz - 1 unspecified Other', '3cd25622-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Efavirenz 30 mg/ml - 180 ml Bottle', '3cd25622-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('EFV+3TC+TDF 600/300/300 mg - 1200 milligram Tablet', '', '78f960da-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Efavirenz (EVF) 600mg + Lamivudine (3TC) 300mg + Tenofovir disoproxil fumarate (TDF) 300mg tablet
            CALL populate_drug('Enalapril - 5 milligram Tablet', '', '4efe3f48-2656-4178-bfcd-c6d103851084'); -- Enalapril maleate, 5mg tablet
            CALL populate_drug('Erthromycine - 500 milligram Tablet', '', 'b3df9e4b-3cd0-4412-a250-4c4e6783d3c2'); -- Erythromycin, 500mg film coated tablet
            CALL populate_drug('Erthromycine - 250 milligram Tablet', '3ccf0b34-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Ethambutol - 400 milligram Tablet', '', 'bd159878-7a33-405b-ad0b-6d6ddbf99ddd'); -- Ethambutol (E), 400mg tablet
            CALL populate_drug('Ethambutol - 1 unspecified Other', '3cd3e910-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Ethionamide - 250 milligram Tablet', '', '78fab5ac-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Ethionamide, 250mg tablet
            CALL populate_drug('Ethionamide - 1 unspecified Other', '3cda2b7c-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('ETV 100mg - 100 milligram Tablet', '', '29c9dd27-25f2-45c6-9708-93fe142a46ba'); -- Etravirine (ETV), 100mg tablet
            CALL populate_drug('Fluconazole - 150 milligram Capsule', '3cd3ec30-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Fluconazole - 200 milligram Tablet', '', '74f565ce-c515-41b2-bbd4-29fc6416231b'); -- Fluconazole, 200mg capsule
            CALL populate_drug('Fluconazole - 50 milligram Tablet', '', 'af8c3c12-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Fluconazole, 50mg capsule
            CALL populate_drug('Fluconazole - 100 milligram Tablet', '3cd3ec30-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Folic Acid - 5 milligram Tablet', '', '6241e56b-ec9c-4c5b-a79c-1ce6481b1acb'); -- Folic acid, 5mg tablet
            CALL populate_drug('Furosemide - 40 milligram Tablet', '', 'fb5842a2-60ef-4539-b428-f99a1f76c85f'); -- Furosemide, 40mg tablet
            CALL populate_drug('Glibenclamide - 5 milligram Tablet', '', '5c21704a-6268-4854-845d-55c573bed967'); -- Glibenclamide, 5mg tablet
            CALL populate_drug('Griseofulvine - 500 milligram Tablet', '', '280353a2-3da7-480b-ab58-03dcd401d3ff'); -- Griseofulvin, 500mg tablet
            CALL populate_drug('Hydrochlorothiazine - 50 milligram Tablet', '3cd90ba2-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Hydrochlorothiazine 25 mg - 25 milligram Tablet', '', 'ce857097-f7a1-4178-b018-a8067a5710d1'); -- Hydrochlorothiazide (HCT), 25mg tablet
            CALL populate_drug('Hydrocortisone Creme - 1 milligram Other', '3ccf30be-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Ibuprofen - 400 milligram Tablet', '', '36e09631-e6d8-41c9-92d2-c945750a39b9'); -- Ibuprofen, 400mg tablet
            CALL populate_drug('Indinavir - 400 milligram Tablet', '3cd3ef46-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Isoniazid - 1 unspecified Other', '3cd27a8a-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Isoniazide - 100 milligram Tablet', '', 'e47fa273-0c52-4f0f-b57b-34001a3e9677'); -- Isoniazid (H), 100mg tablet
            CALL populate_drug('Isoniazide - 300 milligram Tablet', '', '849218ee-901c-46b3-80f9-7c808132893b'); -- Isoniazid (H), 300mg tablet
            CALL populate_drug('Kaletra - 1 unspecified Other', '794AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Kanamycin - 1 gram Other', '3cda2ff0-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Kanamycin - 1 unspecified Other', '3cda2ff0-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Ketoconazol - 200 milligram Tablet', '3cd52762-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Lamivudine - 150 milligram Tablet', '', '78f97264-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine (3TC), 150mg tablet
            CALL populate_drug('Lamivudine - 1 unspecified Other', '3cd24e3e-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Lamivudine 300 mg + Tenofovir 300 mg - 600 milligram Tablet', '161364AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Lamivudine sirop 10mg/ml - 100 ml Bottle', '3cd24e3e-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Levofloxacin - 1 unspecified Other', '3cd3f8c4-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Levofloxacin - 500 milligram Tablet', '3cd3f8c4-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Levofloxacine 250mg - 250 milligram Tablet', '', 'af8c3d3e-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Levofloxacin, 250mg tablet
            CALL populate_drug('Loperamide - 2 milligram Tablet', '', '3c9fa00d-f14f-4bd5-bca8-edeb88cad587'); -- Loperamide hydrochloride, 2mg tablet
            CALL populate_drug('Lopinavir/Ritonavir 100/25 mg - 125 milligram Capsule', '', '78f976c4-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lopinavir (LPV) 100mg + Ritonavir (r) 25mg tablet
            CALL populate_drug('Lopinavir/Ritonavir 200/50 mg - 250 milligram Tablet', '', '78f95e78-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lopinavir (LPV) 200mg + Ritonavir (r) 50mg tablet
            CALL populate_drug('Lopinavir/Ritonavir 40/10 mg - 50 milligram Capsule', '', '78faaaf8-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lopinavir (LPV) 40mg + Ritonavir (r) 10mg, tablet
            CALL populate_drug('Lopinavir/Ritonavir 80/20 mg - 100 milligram Bottle', '794AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Mebendazole - 100 milligram Tablet', '', '73ab0bc6-73ee-486d-b79a-362b423b2233'); -- Mebendazole, 100mg tablet
            CALL populate_drug('Metformin - 850 milligram Tablet', '', 'a40da448-c981-11e7-abc4-cec278b6b50a'); -- Metformin hydrochloride, 850mg tablet
            CALL populate_drug('Metoclopramide - 10 milligram Tablet', '', '399834b3-b44b-11e3-a5e2-0800200c9a66'); -- Metoclopramide hydrochloride, 10mg tablet
            CALL populate_drug('Metronidazole - 500 milligram Ampulle', '', '3015bdd3-4eb4-44a3-91cd-817b79bc4dae'); -- Metronidazole, 500mg vaginal tablet
            CALL populate_drug('Metronidazole - 250 milligram Tablet', '', 'aed7a629-8b95-4942-b665-b35f42abe55d'); -- Metronidazole, 250mg tablet
            CALL populate_drug('Miconazole pommade 2% - 30 milligram Other', '', 'e0389253-0b50-4344-8c11-02d8f4968b8f'); -- Miconazole nitrate, Skin cream, 2%, 30 gram tube
            CALL populate_drug('Multivitamine - 50 milligram Tablet', '3cd12b94-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Nelfinavir - 250 milligram Tablet', '3cd25956-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Nevirapine - 200 milligram Tablet', '', '78f9780e-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Nevirapine (NVP), 200mg tablet
            CALL populate_drug('Nevirapine - 1 unspecified Other', '3cd252f8-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Nevirapine - 50 milligram Tablet', '', '78f96526-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Nevirapine (NVP), 50mg dispersible tablet
            CALL populate_drug('Nevirapine susp 10mg/ml - 100 ml Bottle', '3cd252f8-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Nystatine gttes 100000ui/ml - 30 ml Bottle', '', '9b28cb67-999c-4fc3-aa9c-31237058fe1c'); -- Nystatin, Oral suspension, 100,000 IU/mL, 30mL bottle
            CALL populate_drug('Paracetamol - 500 milligram Tablet', '', '344fea71-ef04-47f8-86f2-0d3937ac4a32'); -- Paracetamol, 500mg tablet
            CALL populate_drug('Paracetamol (120mg/5ml) - 60 ml Bottle', '3cccd4d6-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('PASER - 4 gram Sachet', '71107AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Penicillin Benzathine - 2.4 million_units Ampulle', '', 'ab912f78-2112-4e87-aca7-1ded71e8a04b'); -- Benzathine benzylpenicillin, Powder for solution for injection, 2.4 MIU vial
            CALL populate_drug('Promethazine - 25 milligram Tablet', '', '7965cb77-d7e1-4871-99eb-10718198c869'); -- Promethazine hydrochloride, 25mg coated tablet
            CALL populate_drug('Pyrazinamide - 500 milligram Capsule', '', '79a38c38-5bc8-11e9-8647-d663bd873d93'); -- Pyrazinamide (Z), 500mg tablet
            CALL populate_drug('Pyrazinamide - 400 milligram Tablet', '', '17f95f85-e79c-47b7-b7e8-4c26d77b8dc4'); -- Pyrazinamide (Z), 400mg tablet
            CALL populate_drug('Pyrazinamide - 1 unspecified Other', '3cf0c0f8-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Pyridoxine - 50 milligram Tablet', '', 'bc2f0b43-5a92-4c1a-8004-47e3b36c8bdb'); -- Pyridoxine (Vitamin B6), 50mg tablet
            CALL populate_drug('Pyridoxine - 100 milligram Tablet', '3cd409d6-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('RAL/ETV 400/100 mg - 500 milligram Tablet', '', 'c893970e-22b5-4cb9-8be2-1a9bff480235'); -- Raltegravir (RAL) 400mg + Etravirine (ETV) 100mg, tablet
            CALL populate_drug('RH - 1 unspecified Other', '3cd7d548-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('RH (combination Rifampicin+Isoniazid) 150/100 mg - 250 milligram Tablet', '3cd7d548-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('RH (combination Rifampicin+Isoniazid) 150/75mg - 225 milligram Tablet', '', '85a6e834-0d60-48d1-8847-c42d2e16e57e'); -- Rifampicin (R) 150mg + Isoniazid (H) 75mg tablet
            CALL populate_drug('RH (combination Rifampicin+Isoniazid) 300/150mg - 450 milligram Tablet', '3cd7d548-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('RHEZ - 1 unspecified Other', '3cd769c8-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('RHEZ; ADF(Rifampicin+Isoniazid +Pyrazinamide+Etambutol) - 900 milligram Tablet', '', '79bc3e4f-55dd-46b5-87d9-7f5f6693adf2'); -- Rifampicin (R) 150mg + Isoniazid (H) 75mg + Pyrazinamide (Z) 400mg + Ethambutol (E) 275mg tablet
            CALL populate_drug('Rifabutin - 150 milligram Tablet', '', '93b4e4d6-d986-11e5-b5d2-0a1d41d68578'); -- Rifabutin, 150mg capsule
            CALL populate_drug('Rifabutin - 1 unspecified Other', '3ce42dc0-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Rifampicin - 300 milligram Tablet', '', '78fab944-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Rifampicin (R), 300mg capsule
            CALL populate_drug('Rifampicin - 1 unspecified Other', '3cd40b5c-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Rifampin/Ethambutol - 1 tab Ampulle', '543cd766-7ff2-4a26-ae60-20612821b681', '');
            CALL populate_drug('Ritonavir 100 mg - 100 milligram Tablet', '83412AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Salbutamol aerosol pompre - 100 milligram Other', '3cd4470c-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Salbutamol Nebulizer - 2 ml Other', '3cd4470c-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('SHREZ - 1 unspecified Other', '3e426372-012e-4936-8d57-81bad52f3e3d', '');
            CALL populate_drug('Stavudine - 40 milligram Tablet', '3cd24984-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Stavudine - 1 unspecified Other', '3cd24984-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Streptomycin - 1 gram Ampulle', '', 'f55edf29-b1ac-4c43-8fb4-57a9c4415c0c'); -- Streptomycin sulfate, Powder for solution for injection, 1g vial
            CALL populate_drug('Streptomycin - 1 unspecified Other', '3cd10740-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Sulfate de Fer + HC fol (250mg/60mg) - 310 milligram Tablet', '3cd41552-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Sulphate-de-fer - 200 milligram Capsule', '3ccee960-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Tenofovir - 300 milligram Tablet', '', '78faa576-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Tenofovir disoproxil fumarate (TDF), 300mg tablet
            CALL populate_drug('Tenofovir - 1 unspecified Other', '3cd45166-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Tetracycline HCL - 250 milligram Ampulle', '84894AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', '');
            CALL populate_drug('Vitamin B Complex - 17 milligram Ampulle', '3ccf6e62-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Zidovudine - 300 milligram Capsule', '3cd444be-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Zidovudine - 100 milligram Capsule', '3cd444be-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Zidovudine - 1 unspecified Other', '3cd444be-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Zidovudine sirop 10 mg/ml - 100 ml Bottle', '3cd444be-26fe-102b-80cb-0017a47871b2', '');
            CALL populate_drug('Zidovudine suspension - 240 ml Bottle', '3cd444be-26fe-102b-80cb-0017a47871b2', '');

            DROP PROCEDURE IF EXISTS populate_drug;
        ''')
    }

    void populateOrderReasonForNewOrders() {
        executeMysql("Populate order reason for new orders based on regimen type and medication type", '''

            DROP PROCEDURE IF EXISTS populate_new_order_reason;
            DELIMITER $$ ;
            CREATE PROCEDURE populate_new_order_reason ( _source_product_type varchar(100), _source_regime_type varchar(100), _concept_uuid char(36) )
            BEGIN
                SET @concept_id = (select concept_id from concept where uuid = _concept_uuid);
                IF @concept_id is null THEN
                    update      hivmigration_drug_orders d
                    INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
                    set         d.order_reason_non_coded = concat(_source_product_type, concat(' - ', _source_regime_type))
                    where       d.order_action = 'NEW'
                    and         ((_source_product_type is null and r.source_product_type is null) or _source_product_type = r.source_product_type)
                    and         ((_source_regime_type is null and r.source_regime_type is null) or _source_regime_type = r.source_regime_type);
                ELSE
                    update      hivmigration_drug_orders d
                    INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
                    set         d.order_reason = @concept_id
                    where       d.order_action = 'NEW'
                    and         ((_source_product_type is null and r.source_product_type is null) or _source_product_type = r.source_product_type)
                    and         ((_source_regime_type is null and r.source_regime_type is null) or _source_regime_type = r.source_regime_type);
                END IF;
            END $$
            DELIMITER ;

            CALL populate_new_order_reason('HIV', null, '138405AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Human immunodeficiency virus (HIV) disease
            CALL populate_new_order_reason('HIV', 'accident', '1691AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Post-exposure prophylaxis
            CALL populate_new_order_reason('HIV', 'prophylaxis', '1691AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Post-exposure prophylaxis
            CALL populate_new_order_reason('HIV', 'prophylaxis_rape', '1691AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Post-exposure prophylaxis
            CALL populate_new_order_reason('HIV', 'prophylaxis_sexual_exposure', '1691AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Post-exposure prophylaxis
            CALL populate_new_order_reason('HIV', 'ptme', '160538AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Prevention of maternal to child transmission program
            CALL populate_new_order_reason('OralMed', 'prophylaxis', '1691AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Post-exposure prophylaxis
            CALL populate_new_order_reason('OralMed', 'ptme', '160538AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Prevention of maternal to child transmission program
            CALL populate_new_order_reason('TB', 'ptme', '160538AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Prevention of maternal to child transmission program
            CALL populate_new_order_reason('TB', null, '3ccca7cc-26fe-102b-80cb-0017a47871b2');  -- Tuberculosis
            
            DROP PROCEDURE IF EXISTS populate_new_order_reason;

        ''')
    }

    void populateOrderReasonForDiscontinueOrders() {
        executeMysql("Populate order reason for discontinue orders with discontinue reason", '''

            DROP PROCEDURE IF EXISTS populate_discontinue_order_reason;
            DELIMITER $$ ;
            CREATE PROCEDURE populate_discontinue_order_reason ( _reason varchar(100), _concept_uuid char(36), _non_coded_value varchar(100))
            BEGIN
                SET @concept_id = (select concept_id from concept where uuid = _concept_uuid);
                IF @concept_id is null THEN
                    update      hivmigration_drug_orders d
                    INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
                    set         d.order_reason_non_coded = _non_coded_value
                    where       d.order_action = 'DISCONTINUE'
                    and         r.source_reason_for_closure = _reason;
                ELSE
                    IF _reason is null THEN
                        update      hivmigration_drug_orders d
                        INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
                        set         d.order_reason = @concept_id
                        where       d.order_action = 'DISCONTINUE'
                        and         r.source_reason_for_closure is null;
                    ELSE
                        update      hivmigration_drug_orders d
                        INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
                        set         d.order_reason = @concept_id
                        where       d.order_action = 'DISCONTINUE'
                        and         r.source_reason_for_closure = _reason;
                    END IF;
                END IF;
            END $$
            DELIMITER ;

            CALL populate_discontinue_order_reason('abandoned', '3cdd5176-26fe-102b-80cb-0017a47871b2', null); -- Patient defaulted
            CALL populate_discontinue_order_reason('cd4_augmented', '', 'CD4 augmenté');
            CALL populate_discontinue_order_reason('compliance', '159598AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', null); -- Non-compliance with treatment or therapy
            CALL populate_discontinue_order_reason('died', '3cdd446a-26fe-102b-80cb-0017a47871b2', null); -- Patient died
            CALL populate_discontinue_order_reason('dose_change', '981AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', null); -- Dosing change
            CALL populate_discontinue_order_reason('finished_prophylaxis', '', 'Prophylaxie complété');
            CALL populate_discontinue_order_reason('finished_ptme', '3cd91b56-26fe-102b-80cb-0017a47871b2', null); -- Completed total PMTCT
            CALL populate_discontinue_order_reason('finished_treatment', '3cdcecea-26fe-102b-80cb-0017a47871b2', null); -- Treatment complete
            CALL populate_discontinue_order_reason('formulation_change', '981AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', null); -- Dosing change
            CALL populate_discontinue_order_reason('ineffective', '3cd49432-26fe-102b-80cb-0017a47871b2', null);  -- Regimen failure
            CALL populate_discontinue_order_reason('lost', '3cdd5176-26fe-102b-80cb-0017a47871b2', null); -- Patient defaulted
            CALL populate_discontinue_order_reason('moved_in_haiti_txfer', '', 'Déplacé à l\\'interieur du pays');
            CALL populate_discontinue_order_reason('moved_out_haiti_txfe', '', 'Déplacé à l\\'extérieur du pays');
            CALL populate_discontinue_order_reason('other', '3cee7fb4-26fe-102b-80cb-0017a47871b2', null); -- Other non-coded
            CALL populate_discontinue_order_reason('pregnancy', '3cdd8132-26fe-102b-80cb-0017a47871b2', null); -- Patient pregnant
            CALL populate_discontinue_order_reason('prev_undoc_txfer_con', '', 'Transfert précédemment non documenté, confirmé');
            CALL populate_discontinue_order_reason('prev_undoc_txfer_not', '', 'Transfert précédemment non documenté, non confirmé');
            CALL populate_discontinue_order_reason('refused_return_to_tr', 'efab937b-853e-47da-b97e-220f1bdff97d', null); -- Refusal of treatment by patient
            CALL populate_discontinue_order_reason('resistant', '', 'Résistant');
            CALL populate_discontinue_order_reason('side_effect', '3cccecdc-26fe-102b-80cb-0017a47871b2', null); -- Toxicity, drug
            CALL populate_discontinue_order_reason('stock_out', '3cde143a-26fe-102b-80cb-0017a47871b2', null); -- Medications unavailable
            CALL populate_discontinue_order_reason('tb', '', 'Tuberculose');
            CALL populate_discontinue_order_reason('traced_not_found', '', 'Recherché, non retrouvé');
            CALL populate_discontinue_order_reason('transferred_out', '3cdd5c02-26fe-102b-80cb-0017a47871b2', null); -- Patient transferred out
            CALL populate_discontinue_order_reason('treatment_refused', 'efab937b-853e-47da-b97e-220f1bdff97d', null); -- Refusal of treatment by patient
            CALL populate_discontinue_order_reason('treatment_stopped_ot', '3cdc0d7a-26fe-102b-80cb-0017a47871b2', null); -- Treatment stopped
            CALL populate_discontinue_order_reason('tx_stop_side_effects', '3cccecdc-26fe-102b-80cb-0017a47871b2', null); -- Toxicity, drug
            CALL populate_discontinue_order_reason(null, '3cd743f8-26fe-102b-80cb-0017a47871b2', null); -- None

            DROP PROCEDURE IF EXISTS populate_discontinue_order_reason;
        ''')
    }

    void populateDosingInstructions() {
        executeMysql("Populate dosing instructions from legacy data", '''

            UPDATE      hivmigration_drug_orders d
            INNER JOIN  hivmigration_regimes r on r.source_regime_id = d.source_regime_id
            SET d.as_needed = r.source_prn, d.dosing_instructions = concat(
                r.source_ddd, ' /day', ' x ', source_dwd, ' days/week',
                if (r.source_morning_dose > 0, concat(' - ', r.source_morning_dose, '/morning'), ''),
                if (r.source_noon_dose > 0, concat(' - ', r.source_noon_dose, '/noon'), ''),
                if (r.source_evening_dose > 0, concat(' - ', r.source_evening_dose, '/evening'), ''),
                if (r.source_night_dose > 0, concat(' - ', r.source_night_dose, '/night'), '')
            );
            
        ''')
    }

    void createOrders() {
        executeMysql("Insert data into the Order table", '''

            SET @drug_order_type = (SELECT order_type_id FROM order_type WHERE uuid = '131168f4-15f5-102d-96e4-000c29c2a5d7');
            SET @outpatient_care_setting = (SELECT care_setting_id FROM care_setting WHERE uuid = '6f0c9a92-6f24-11e3-af88-005056821db0');
            SET @unknown_provider = (SELECT provider_id FROM provider WHERE uuid = 'f9badd80-ab76-11e2-9e96-0800200c9a66');

            insert into orders (
                order_id, uuid, order_type_id, patient_id, encounter_id, 
                order_number, previous_order_id,
                order_action, date_activated, urgency, scheduled_date, 
                auto_expire_date, date_stopped, 
                care_setting, concept_id, order_reason, order_reason_non_coded,
                orderer, creator, date_created, voided
            )
            select
                d.order_id, d.order_uuid, @drug_order_type, d.patient_id, d.encounter_id, 
                concat('HIVEMR-', d.source_regime_id), d.previous_order_id,
                d.order_action, d.date_activated, d.urgency, if(d.urgency = 'ON_SCHEDULED_DATE', d.start_date, null), 
                d.auto_expire_date, d.date_stopped,
                @outpatient_care_setting, d.concept_id, d.order_reason, d.order_reason_non_coded,
                @unknown_provider, 1, date_format(curdate(), '%Y-%m-%d %T'), 0
            from 
                hivmigration_drug_orders d
            where d.concept_id is not null
            ; 
            
        ''')
    }

    void createDrugOrders() {
        executeMysql("Insert data into the Drug Order table", '''

            insert into drug_order (
                order_id, drug_inventory_id, drug_non_coded, dosing_type, dosing_instructions, as_needed
            )
            select
                d.order_id, d.drug_id, d.drug_non_coded, 'org.openmrs.FreeTextDosingInstructions', d.dosing_instructions, d.as_needed
            from 
                hivmigration_drug_orders d
            ; 
            
        ''')
    }

    void removeRegimenEncountersThatHaveNoAssociatedDrugOrders() {
        executeMysql("Remove regime encounters that have no orders associated with them", '''
            delete e.* from encounter e
            inner join encounter_type t on e.encounter_type = t.encounter_type_id
            where t.uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002'
            and e.encounter_id not in (select distinct encounter_id from orders);
        ''')
    }

    @Override
    def void revert() {
        if (tableExists("hivmigration_drug_orders")) {
            executeMysql("Remove drug orders, orders, and encounters created", '''
                
                delete from drug_order where order_id in (select order_id from hivmigration_drug_orders where previous_order_id is not null);
                delete from orders where order_id in (select order_id from hivmigration_drug_orders where previous_order_id is not null);
                delete from drug_order where order_id in (select order_id from hivmigration_drug_orders);
                delete from orders where order_id in (select order_id from hivmigration_drug_orders);
                
                delete     e 
                from       encounter e
                inner join (select patient_id, date_activated from hivmigration_drug_orders where encounter_id is null group by patient_id, date_activated) x
                on         e.patient_id = x.patient_id
                and        e.encounter_datetime = x.date_activated
                where      e.encounter_type = (select encounter_type_id from encounter_type where uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002');
            ''')
            executeMysql("drop table hivmigration_drug_orders")
        }
        executeMysql("drop table if exists hivmigration_regimes")
    }

    void validateResults() {

        assertMatch(
                "All data from Oracle should be loaded into staging table",
                "select count(*) as num from hiv_regimes_real r, hiv_demographics_real d where r.patient_id = d.patient_id",
                "select count(*) as num from hivmigration_regimes"
        )

        assertMatch(
                "There should be a new order for all regimes",
                "select count(*) as num from hiv_regimes_real r, hiv_demographics_real d where r.patient_id = d.patient_id",
                "select count(*) as num from orders where order_action = 'NEW'"
        )

        assertSetsMatch(
                "There should be a discontinue order for all closed regimes where close date is not in the future",
                "hivemr_id",
                "select d.patient_id as hivemr_id from hiv_regimes_real r, hiv_demographics_real d where r.patient_id = d.patient_id and close_date is not null and close_date <= sysdate",
                "select patient_identifier(patient_id, 'HIVEMR-V1') as hivemr_id from orders where order_action = 'DISCONTINUE'"
        )

        assertNoRows(
                "All discontinue orders must have a previous order id",
                "select count(*) from orders where order_action = 'DISCONTINUE' and previous_order_id is null",
        )

        assertAllRows(
                "All drug orders must have a coded or non-coded drug", "drug_order",
                "select count(*) as num from drug_order where drug_inventory_id is not null or drug_non_coded is not null"
        )

        assertAllRows(
                "All orders must be routine or scheduled","drug_order",
                "select count(*) as num from orders o where (urgency = 'ROUTINE' and scheduled_date is null) or (urgency = 'ON_SCHEDULED_DATE' and scheduled_date > date_activated)"
        )

        assertNoRows(
                "All discontinue orders should have an order reason (coded or non-coded)",
                "select count(*) as num from orders o where order_action = 'DISCONTINUE' and order_reason is null and order_reason_non_coded is null"
        )

        // From OrderValidator.java

        assertNoRows(
                "All orders must have non-null voided",
                "select count(*) as num from orders where voided is null"
        )

        assertNoRows(
                "All orders must have non-null concept",
                "select count(*) as num from orders where concept_id is null"
        )

        assertNoRows(
                "All orders must have non-null patient",
                "select count(*) as num from orders where patient_id is null"
        )

        assertNoRows(
                "All orders must have non-null encounter",
                "select count(*) as num from orders where encounter_id is null"
        )

        assertNoRows(
                "All orders must have non-null orderer",
                "select count(*) as num from orders where orderer is null"
        )

        assertNoRows(
                "All orders must have non-null urgency",
                "select count(*) as num from orders where urgency is null"
        )

        assertNoRows(
                "All orders must have non-null action",
                "select count(*) as num from orders where order_action is null"
        )

        assertAllRows(
                "All orders must have the same patient in the order and the encounter","drug_order",
                "select count(*) as num from orders o inner join encounter e on e.encounter_id = o.encounter_id where o.patient_id = e.patient_id"
        )

        assertAllRows(
                "All orders must have date activated on or after encounter date", "drug_order",
                "select count(*) as num from orders o inner join encounter e on o.encounter_id = e.encounter_id where o.date_activated >= e.encounter_datetime"
        )

        assertNoRows(
                "No orders can have date activated in the future",
                "select count(*) from orders where date_activated > now()"
        )

        assertNoRows(
                "No orders can have date activated after date stopped",
                "select count(*) from orders where date_stopped is not null and date_activated > date_stopped"
        )

        assertNoRows(
                "No orders can have date activated after auto expire date",
                "select count(*) from orders where auto_expire_date is not null and date_activated > auto_expire_date"
        )

        assertNoRows(
                "No orders can have a scheduled date unless urgency is on scheduled date",
                "select count(*) from orders where scheduled_date is not null and urgency != 'ON_SCHEDULED_DATE'"
        )

        assertNoRows(
                "No orders can have an urgency of on scheduled date if scheduled date is null",
                "select count(*) from orders where scheduled_date is null and urgency = 'ON_SCHEDULED_DATE'"
        )

        // From DrugOrderValidator.java

        assertNoRows(
                "All rows must have a non-null as_needed",
                "select count(*) from drug_order where as_needed is null"
        )

        assertNoRows(
                "Dosing type is required if action != DISCONTINUE",
                "select count(*) from orders o inner join drug_order d on o.order_id = d.order_id where dosing_type is null and order_action != 'DISCONTINUE'"
        )

        assertNoRows(
                "Concept associated with drug must not have a different concept than the one associated with the order",
                "select count(*) from drug_order dro inner join orders o on dro.order_id = o.order_id inner join drug d on dro.drug_inventory_id = d.drug_id where d.concept_id != o.concept_id"
        )

        assertAllRows(
                "All rows are outpatient","drug_order",
                "select count(*) from orders o inner join care_setting c on o.care_setting = c.care_setting_id where c.care_setting_type = 'OUTPATIENT'"
        )

        assertAllRows(
                "All drug orders must have a coded or non-coded drug","drug_order",
                "select count(*) as num from drug_order where drug_inventory_id is not null or drug_non_coded is not null"
        )

        assertNoRows(
                "No drug orders should have both a coded and non-coded drug",
                "select count(*) as num from drug_order where drug_inventory_id is not null and drug_non_coded is not null"
        )

        // From FreeTextDosingInstructions validator

        assertNoRows(
                "Non-discontinue orders with free text dosing instructions must have non-null dosing instructions",
                "select count(*) from drug_order d inner join orders o on d.order_id = o.order_id where order_action != 'DISCONTINUE' and dosing_instructions is null"
        )
    }
}
