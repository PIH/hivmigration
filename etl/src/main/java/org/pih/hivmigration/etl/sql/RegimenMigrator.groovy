package org.pih.hivmigration.etl.sql

class RegimenMigrator extends SqlMigrator {

    @Override
    void migrate() {

        createStagingTable()
        setAutoIncrements()
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


    }

    void createStagingTable() {
        executeMysql("Create staging table", '''
            create table hivmigration_drug_orders (
            
              -- Source data
              order_id int PRIMARY KEY AUTO_INCREMENT,
              source_regime_id int, -- This will mostly be used to populate previous order id on the discontinue order
              order_action varchar(15), -- For a given source_regime_id, there will be one row with NEW, and 0-1 with DISCONTINUE
              source_encounter_id int,
              source_patient_id int,
              source_product_id int,
              effective_start_date date, -- this should be close_date when adding DISCONTINUE, start_date when adding NEW
              effective_start_date_estimated boolean, -- there is nothing to import this into, but import this in from close_date_estimated_p
              date_stopped date, -- This comes from close_date
              auto_expire_date date, -- this comes from end_date
              source_product_type varchar(20), -- this comes from the hiv_products table joined on hiv_regimes
              source_discontinue_reason varchar(20), -- from reason_for_closure
              source_regime_type varchar(50), -- from regime_type
              ddd double, -- daily dose
              dwd double, -- weekly dose
              morning_dose double,
              noon_dose double,
              evening_dose double,
              night_dose double,
              
              -- Derived data
              order_uuid char(38), -- This needs to get populated with the uuid() function
              encounter_id int,
              patient_id int,
              previous_order_id int,
              drug_id int, -- This should be mapped in based on the source_product_id
              concept_id int, -- This can be auto-populated from the drug_id 
              date_activated date, -- This ultimately needs to be populated to match the encounter_date that gets created
              scheduled_date date, -- This should match the effective_start_date if effective_start_date != date_activated
              urgency varchar(20),
              order_reason int, -- This should be mapped based on what is in the source_reason_for_closure, source regime_type, and source_product_type
              order_reason_non_coded varchar(100),
              dosing_instructions varchar(100),
              
              KEY `source_regime_id_idx` (`source_regime_id`),
              KEY `source_patient_id_idx` (`source_patient_id`),
              KEY `source_encounter_id_idx` (`source_encounter_id`),
              KEY `source_product_id_idx` (`source_product_id`),
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

    void loadStagingTableWithNewOrders() {
        loadFromOracleToMySql('''
            insert into hivmigration_drug_orders (
              source_regime_id,
              order_action,
              source_encounter_id,
              source_patient_id,
              source_product_id,
              effective_start_date,
              date_stopped,
              auto_expire_date,
              source_product_type,
              source_regime_type,
              ddd,
              dwd,
              morning_dose,
              noon_dose,
              evening_dose,
              night_dose
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', '''
            select
                   r.regime_id as source_regime_id,
                   'NEW' as order_action,
                   e.encounter_id as source_encounter_id,
                   r.patient_id as source_patient_id,
                   r.product_id as source_product_id,
                   r.start_date as effective_start_date,
                   r.close_date as date_stopped,
                   r.end_date as auto_expire_date,
                   p.prod_type_flag as source_product_type,
                   r.regime_type as source_regime_type,
                   r.ddd,
                   r.dwd,
                   r.morning_dose,
                   r.noon_dose,
                   r.evening_dose,
                   r.night_dose
            from
                 hiv_regimes_real r, hiv_products p, hiv_encounters e, hiv_demographics_real d
            where
                  r.product_id = p.product_id
            and   r.patient_id = d.patient_id
            and   r.encounter_opened_by = e.encounter_id(+)
            ;
        ''')
    }

    void loadStagingTableWithDiscontinueOrders() {
        loadFromOracleToMySql('''
            insert into hivmigration_drug_orders (
              source_regime_id,
              order_action,
              source_encounter_id,
              source_patient_id,
              source_product_id,
              effective_start_date,
              effective_start_date_estimated,
              auto_expire_date,
              source_discontinue_reason
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', '''
            select
                   r.regime_id as source_regime_id,
                   'DISCONTINUE' as order_action,
                   e.encounter_id as source_encounter_id,
                   r.patient_id as source_patient_id,
                   r.product_id as source_product_id,
                   r.close_date as effective_start_date,
                   decode(r.close_date_estimated_p, 't', 1, 0) as effective_start_date_estimated,
                   r.close_date as auto_expire_date,
                   r.reason_for_closure as source_discontinue_reason
            from
                 hiv_regimes_real r, hiv_products p, hiv_encounters e, hiv_demographics_real d
            where
                  r.product_id = p.product_id
            and   r.patient_id = d.patient_id
            and   r.close_date is not null
            and   r.encounter_closed_by = e.encounter_id(+)
            ;
        ''')
    }

    void populateStagingTableWithDerivedValues() {
        executeMysql("Populate uuids", '''
            update hivmigration_drug_orders set order_uuid = uuid()
        ''')

        executeMysql("Populate patient ids", '''
            update  hivmigration_drug_orders d, hivmigration_patients p 
            set     d.patient_id = p.person_id where d.source_patient_id = p.source_patient_id
        ''')

        executeMysql("Populate encounter ids migrated by encounter migrator", '''
            update  hivmigration_drug_orders d, hivmigration_encounters e 
            set     d.encounter_id = e.encounter_id where d.source_encounter_id = e.source_encounter_id
        ''')

        executeMysql("Populate previous order ids", '''
            update  hivmigration_drug_orders d, hivmigration_drug_orders n
            set     d.previous_order_id = n.order_id
            where   d.order_action = 'DISCONTINUE' and n.order_action = 'NEW'
            and     d.source_regime_id = n.source_regime_id
        ''')

        executeMysql("Populate date activated on orders associated with encounters", '''
            update  hivmigration_drug_orders o, 
                    (select source_encounter_id, min(effective_start_date) as first_start_date from hivmigration_drug_orders group by source_encounter_id) a
            set     o.date_activated = a.first_start_date
            where   o.source_encounter_id is not null 
            and     o.source_encounter_id = a.source_encounter_id
        ''')

        executeMysql("Populate date activated on orders not associated with encounters", '''
            update  hivmigration_drug_orders o
            set     o.date_activated = o.effective_start_date
            where   o.source_encounter_id is null 
        ''')

        executeMysql("Set urgency to ROUTINE if date activated matches effective start date", '''
            update  hivmigration_drug_orders o
            set     o.urgency = 'ROUTINE'
            where   o.date_activated = o.effective_start_date
        ''')

        executeMysql("Populate scheduled_date and urgency if date activated < effective start date", '''
            update  hivmigration_drug_orders o
            set     o.urgency = 'ON_SCHEDULED_DATE', o.scheduled_date = o.effective_start_date
            where   o.date_activated < o.effective_start_date
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
        ''')
    }

    void populateDrugAndConceptIdsForEachProductId() {
        executeMysql("Populate drug ids that map to each product id", '''
            DROP PROCEDURE IF EXISTS populate_drug_and_concept;
            DELIMITER $$ ;
            CREATE PROCEDURE populate_drug_and_concept ( _product_id int, _drug_uuid char(36))
            BEGIN
                SET @drug_id = (select drug_id from drug where uuid = _drug_uuid);
                SET @concept_id = (select concept_id from drug where uuid = _drug_uuid);
                update hivmigration_drug_orders set drug_id = @drug_id, concept_id = @concept_id where source_product_id = _product_id;
            END $$
            DELIMITER ;

            CALL populate_drug_and_concept(2217, '78f98308-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Abacavir 20 mg/ml 20milligram Other -> Abacavir sulfate (ABC), Oral solution, 20mg/mL, 240mL bottle
            CALL populate_drug_and_concept(56, '78f981d2-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Abacavir 300 mg 300milligram Tablet -> Abacavir sulfate (ABC), 300mg tablet
            CALL populate_drug_and_concept(2477, '35c6041e-0af3-4bab-887d-9db682a02248'); -- Abacavir 60 mg 60milligram Tablet -> Abacavir (ABC) sulfate, 60mg dispersible tablet
            CALL populate_drug_and_concept(2575, 'a6985b15-5fd6-4bdf-93f6-62930a438464'); -- ABC/3TC 120/60 mg 180milligram Tablet -> Abacavir (ABC) sulfate 120mg + Lamivudine (3TC) 60mg, tablet for oral suspension
            CALL populate_drug_and_concept(2475, 'f2a2c2d9-16a6-4138-9074-6fdf3307e107'); -- ABC/3TC 300/300 mg 600milligram Capsule -> Abacavir sulfate (ABC) 300mg + Lamivudine (3TC) 300mg tablet
            CALL populate_drug_and_concept(2476, '78faa9b8-dfbe-11e9-8a34-2a2ae2dbcce4'); -- ABC/3TC 60/30 mg 90milligram Capsule -> Abacavir sulfate (ABC) 60mg + Lamivudine (3TC) 30mg, tablet for oral suspension
            CALL populate_drug_and_concept(1904, '');
            CALL populate_drug_and_concept(109, '8d5941e8-5f86-4289-a6ba-122320814bd5'); -- Acide Acétylsalycilique 100milligram Tablet -> Acetylsalicylic acid, 100mg tablet
            CALL populate_drug_and_concept(27, '0962492b-295e-4f61-8d7b-717a61f43997'); -- Acyclovir 200milligram Tablet -> Aciclovir, 200mg tablet
            CALL populate_drug_and_concept(1544, '');
            CALL populate_drug_and_concept(132, '8112cd30-ae2a-11e4-ab27-0800200c9a66'); -- Albendazole 400milligram Tablet -> Albendazole, 400mg chewable tablet
            CALL populate_drug_and_concept(107, '1491ae88-f38c-4a8c-94d5-f8385eb3b9d9'); -- Amoxycillin 500milligram Tablet -> Amoxicillin, 500mg tablet
            CALL populate_drug_and_concept(48, '');
            CALL populate_drug_and_concept(2435, '');
            CALL populate_drug_and_concept(2495, '78f95d38-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Atazanavir/Ritonavir 300/100 mg 400milligram Tablet -> Atazanavir sulfate (ATV) 300mg + Ritonavir (r) 100mg tablet
            CALL populate_drug_and_concept(110, '2f8d7a99-d4ec-4ad7-b898-1c953cb332fd'); -- Atenolol 50milligram Tablet -> Atenolol, 50mg tablet
            CALL populate_drug_and_concept(42, '78f968e6-dfbe-11e9-8a34-2a2ae2dbcce4'); -- AZT+3TC 300/150 mg 450milligram Tablet -> Lamivudine (3TC) 150mg + Zidovudine (AZT) 300mg tablet
            CALL populate_drug_and_concept(2386, '78f95fa4-dfbe-11e9-8a34-2a2ae2dbcce4'); -- AZT+3TC 60/30 mg 90milligram Tablet -> Lamivudine (3TC) 30mg + Zidovudine (AZT) 60mg tablet
            CALL populate_drug_and_concept(2378, '78f9739a-dfbe-11e9-8a34-2a2ae2dbcce4'); -- AZT+3TC+NVP 300/150/200 mg 650milligram Tablet -> Lamivudine (3TC) 150mg + Nevirapine (NVP) 200mg + Zidovudine (AZT) 300mg tablet
            CALL populate_drug_and_concept(2380, '78f97cfa-dfbe-11e9-8a34-2a2ae2dbcce4'); -- AZT+3TC+NVP 60/30/50 mg 140milligram Tablet -> Lamivudine (3TC) 30mg + Nevirapine (NVP) 50mg + Zidovudine (AZT) 60mg dispersible tablet
            CALL populate_drug_and_concept(2407, '');
            CALL populate_drug_and_concept(245, '2896a1ad-f576-4073-875e-835d40d70548'); -- Benzyl PNC 5,000,000IU 1gram Other -> Benzylpenicillin (Penicillin G), Powder for solution for injection, 5 MIU vial
            CALL populate_drug_and_concept(111, '');
            CALL populate_drug_and_concept(2236, '');
            CALL populate_drug_and_concept(64, '');
            CALL populate_drug_and_concept(112, '');
            CALL populate_drug_and_concept(113, 'e371d811-d32c-4f6e-8493-2fa667b7b44c'); -- Carbamazépine 200milligram Tablet -> Carbamazepine, 200mg film coated tablet
            CALL populate_drug_and_concept(114, '4c908591-3adf-4601-b61c-4faddffbee56'); -- Cephalexine 500milligram Tablet -> Cefalexin, 500mg capsule
            CALL populate_drug_and_concept(1499, '');
            CALL populate_drug_and_concept(120, 'a0b5fc86-543f-4162-93a7-8936a565a172'); -- Chloroquine 150milligram Tablet -> Chloroquine, 150mg base (242mg phosphate) tablet
            CALL populate_drug_and_concept(28, 'ef96f590-2cc3-469e-9b82-072fef563b9e'); -- Cimetidine 200milligram Tablet -> Cimetidine, 200mg tablet
            CALL populate_drug_and_concept(122, '2a885fd0-f8c9-43fb-883b-fe25d5769338'); -- Ciprofloxacin 500milligram Tablet -> Ciprofloxacin, 500mg film coated tablet
            CALL populate_drug_and_concept(124, '');
            CALL populate_drug_and_concept(123, '54972d88-156e-465e-8483-9a9e97d5898f'); -- Cotrimoxazol (400/80mg) 1tab Tablet -> Cotrimoxazole (Sulfamethoxazole/Trimethoprim), 400mg/80mg tablet
            CALL populate_drug_and_concept(23, '85153088-b868-4723-aacd-27f25f121685'); -- Cotrimoxazol (800/160mg) 1tab Tablet -> Cotrimoxazole (Sulfamethoazxole/Trimethoprim), 800mg/160mg tablet
            CALL populate_drug_and_concept(2356, '160d7a20-f710-48b3-9c9b-0a1b98ab5871'); -- Cotrimoxazol TMP/ SMX 120milligram Tablet -> Cotrimoxazole (Sulfamethoxazole/Trimethoprim), 100mg/20mg tablet
            CALL populate_drug_and_concept(125, '');
            CALL populate_drug_and_concept(2238, '');
            CALL populate_drug_and_concept(63, '78fab188-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Cycloserine 250milligram Tablet -> Cycloserine, 250mg capsule
            CALL populate_drug_and_concept(1632, '78f97e3a-dfbe-11e9-8a34-2a2ae2dbcce4'); -- D4T+3TC+NVP ( Triomune-30) 380milligram Tablet -> Lamivudine (3TC) 150mg + Stavudine (d4T) 30mg + Nevirapine (NVP) 200mg tablet
            CALL populate_drug_and_concept(127, '1156a9ca-14f3-4c57-9ed2-7154e82447c7'); -- Dapsone 100milligram Tablet -> Dapsone, 100mg tablet
            CALL populate_drug_and_concept(267, '');
            CALL populate_drug_and_concept(1615, '');
            CALL populate_drug_and_concept(130, 'e44e484e-90ec-41ca-aec1-e3190d7be626'); -- Digoxine 0.25milligram Tablet -> Digoxin, 250 microgram tablet
            CALL populate_drug_and_concept(2455, '78fab02a-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Dolutegravir 50milligram Tablet -> Dolutegravir (DTG), 50 mg tablet
            CALL populate_drug_and_concept(129, '8aad2a23-2977-4b5b-a30a-4a9142ce774b'); -- Doxycycline 100milligram Tablet -> Doxycycline, 100mg tablet
            CALL populate_drug_and_concept(2516, '');
            CALL populate_drug_and_concept(2456, '78faac2e-dfbe-11e9-8a34-2a2ae2dbcce4'); -- DTG/3TC/TDF 50/300/300 mg 650milligram Tablet -> Dolutegravir (DTG) 50mg + Lamivudine (3TC) 300mg + Tenofovir disoproxil fumarate (TDF) 300mg, tablet
            CALL populate_drug_and_concept(61, '78f96210-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Efavirenz 600milligram Tablet -> Efavirenz (EFV), 600mg tablet
            CALL populate_drug_and_concept(7, '78f97b9c-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Efavirenz 200milligram Tablet -> Efavirenz (EFV), 200mg tablet
            CALL populate_drug_and_concept(2219, '');
            CALL populate_drug_and_concept(2381, '');
            CALL populate_drug_and_concept(2379, '78f960da-dfbe-11e9-8a34-2a2ae2dbcce4'); -- EFV+3TC+TDF 600/300/300 mg 1200milligram Tablet -> Efavirenz (EVF) 600mg + Lamivudine (3TC) 300mg + Tenofovir disoproxil fumarate (TDF) 300mg tablet
            CALL populate_drug_and_concept(294, '4efe3f48-2656-4178-bfcd-c6d103851084'); -- Enalapril 5milligram Tablet -> Enalapril maleate, 5mg tablet
            CALL populate_drug_and_concept(1574, 'b3df9e4b-3cd0-4412-a250-4c4e6783d3c2'); -- Erthromycine 500milligram Tablet -> Erythromycin, 500mg film coated tablet
            CALL populate_drug_and_concept(136, '');
            CALL populate_drug_and_concept(24, 'bd159878-7a33-405b-ad0b-6d6ddbf99ddd'); -- Ethambutol 400milligram Tablet -> Ethambutol (E), 400mg tablet
            CALL populate_drug_and_concept(2239, '');
            CALL populate_drug_and_concept(62, '78fab5ac-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Ethionamide 250milligram Tablet -> Ethionamide, 250mg tablet
            CALL populate_drug_and_concept(2240, '');
            CALL populate_drug_and_concept(2536, '29c9dd27-25f2-45c6-9708-93fe142a46ba'); -- ETV 100mg 100milligram Tablet -> Etravirine (ETV), 100mg tablet
            CALL populate_drug_and_concept(8, '');
            CALL populate_drug_and_concept(1669, '74f565ce-c515-41b2-bbd4-29fc6416231b'); -- Fluconazole 200milligram Tablet -> Fluconazole, 200mg capsule
            CALL populate_drug_and_concept(46, 'af8c3c12-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Fluconazole 50milligram Tablet -> Fluconazole, 50mg capsule
            CALL populate_drug_and_concept(1817, '');
            CALL populate_drug_and_concept(138, '6241e56b-ec9c-4c5b-a79c-1ce6481b1acb'); -- Folic Acid 5milligram Tablet -> Folic acid, 5mg tablet
            CALL populate_drug_and_concept(139, 'fb5842a2-60ef-4539-b428-f99a1f76c85f'); -- Furosemide 40milligram Tablet -> Furosemide, 40mg tablet
            CALL populate_drug_and_concept(295, '5c21704a-6268-4854-845d-55c573bed967'); -- Glibenclamide 5milligram Tablet -> Glibenclamide, 5mg tablet
            CALL populate_drug_and_concept(143, '280353a2-3da7-480b-ab58-03dcd401d3ff'); -- Griseofulvine 500milligram Tablet -> Griseofulvin, 500mg tablet
            CALL populate_drug_and_concept(146, '');
            CALL populate_drug_and_concept(2075, 'ce857097-f7a1-4178-b018-a8067a5710d1'); -- Hydrochlorothiazine 25 mg 25milligram Tablet -> Hydrochlorothiazide (HCT), 25mg tablet
            CALL populate_drug_and_concept(184, '');
            CALL populate_drug_and_concept(37, '36e09631-e6d8-41c9-92d2-c945750a39b9'); -- Ibuprofen 400milligram Tablet -> Ibuprofen, 400mg tablet
            CALL populate_drug_and_concept(9, '');
            CALL populate_drug_and_concept(2241, '');
            CALL populate_drug_and_concept(21, 'e47fa273-0c52-4f0f-b57b-34001a3e9677'); -- Isoniazide 100milligram Tablet -> Isoniazid (H), 100mg tablet
            CALL populate_drug_and_concept(265, '849218ee-901c-46b3-80f9-7c808132893b'); -- Isoniazide 300milligram Tablet -> Isoniazid (H), 300mg tablet
            CALL populate_drug_and_concept(2225, '');
            CALL populate_drug_and_concept(238, '');
            CALL populate_drug_and_concept(2242, '');
            CALL populate_drug_and_concept(148, '');
            CALL populate_drug_and_concept(10, '78f97264-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lamivudine 150milligram Tablet -> Lamivudine (3TC), 150mg tablet
            CALL populate_drug_and_concept(2216, '');
            CALL populate_drug_and_concept(2377, '');
            CALL populate_drug_and_concept(1634, '');
            CALL populate_drug_and_concept(2243, '');
            CALL populate_drug_and_concept(1996, '');
            CALL populate_drug_and_concept(2179, 'af8c3d3e-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Levofloxacine 250mg 250milligram Tablet -> Levofloxacin, 250mg tablet
            CALL populate_drug_and_concept(43, '3c9fa00d-f14f-4bd5-bca8-edeb88cad587'); -- Loperamide 2milligram Tablet -> Loperamide hydrochloride, 2mg tablet
            CALL populate_drug_and_concept(1614, '78f976c4-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lopinavir/Ritonavir 100/25 mg 125milligram Capsule -> Lopinavir (LPV) 100mg + Ritonavir (r) 25mg tablet
            CALL populate_drug_and_concept(1909, '78f95e78-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lopinavir/Ritonavir 200/50 mg 250milligram Tablet -> Lopinavir (LPV) 200mg + Ritonavir (r) 50mg tablet
            CALL populate_drug_and_concept(2478, '78faaaf8-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Lopinavir/Ritonavir 40/10 mg 50milligram Capsule -> Lopinavir (LPV) 40mg + Ritonavir (r) 10mg, tablet
            CALL populate_drug_and_concept(2479, '');
            CALL populate_drug_and_concept(150, '73ab0bc6-73ee-486d-b79a-362b423b2233'); -- Mebendazole 100milligram Tablet -> Mebendazole, 100mg tablet
            CALL populate_drug_and_concept(151, 'a40da448-c981-11e7-abc4-cec278b6b50a'); -- Metformin 850milligram Tablet -> Metformin hydrochloride, 850mg tablet
            CALL populate_drug_and_concept(154, '399834b3-b44b-11e3-a5e2-0800200c9a66'); -- Metoclopramide 10milligram Tablet -> Metoclopramide hydrochloride, 10mg tablet
            CALL populate_drug_and_concept(1556, '');
            CALL populate_drug_and_concept(155, '');
            CALL populate_drug_and_concept(1506, '');
            CALL populate_drug_and_concept(157, '');
            CALL populate_drug_and_concept(47, '');
            CALL populate_drug_and_concept(13, '78f9780e-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Nevirapine 200milligram Tablet -> Nevirapine (NVP), 200mg tablet
            CALL populate_drug_and_concept(2220, '');
            CALL populate_drug_and_concept(1818, '78f96526-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Nevirapine 50milligram Tablet -> Nevirapine (NVP), 50mg dispersible tablet
            CALL populate_drug_and_concept(1633, '');
            CALL populate_drug_and_concept(1604, '9b28cb67-999c-4fc3-aa9c-31237058fe1c'); -- Nystatine gttes 100000ui/ml 30ml Bottle -> Nystatin, Oral suspension, 100,000 IU/mL, 30mL bottle
            CALL populate_drug_and_concept(162, '344fea71-ef04-47f8-86f2-0d3937ac4a32'); -- Paracetamol 500milligram Tablet -> Paracetamol, 500mg tablet
            CALL populate_drug_and_concept(1576, '');
            CALL populate_drug_and_concept(174, '');
            CALL populate_drug_and_concept(29, '');
            CALL populate_drug_and_concept(168, '7965cb77-d7e1-4871-99eb-10718198c869'); -- Promethazine 25milligram Tablet -> Promethazine hydrochloride, 25mg coated tablet
            CALL populate_drug_and_concept(25, '79a38c38-5bc8-11e9-8647-d663bd873d93'); -- Pyrazinamide 500milligram Capsule -> Pyrazinamide (Z), 500mg tablet
            CALL populate_drug_and_concept(1681, '17f95f85-e79c-47b7-b7e8-4c26d77b8dc4'); -- Pyrazinamide 400milligram Tablet -> Pyrazinamide (Z), 400mg tablet
            CALL populate_drug_and_concept(2247, '');
            CALL populate_drug_and_concept(30, 'bc2f0b43-5a92-4c1a-8004-47e3b36c8bdb'); -- Pyridoxine 50milligram Tablet -> Pyridoxine (Vitamin B6), 50mg tablet
            CALL populate_drug_and_concept(169, '');
            CALL populate_drug_and_concept(2515, 'c893970e-22b5-4cb9-8be2-1a9bff480235'); -- RAL/ETV 400/100 mg 500milligram Tablet -> Raltegravir (RAL) 400mg + Etravirine (ETV) 100mg, tablet
            CALL populate_drug_and_concept(2230, '');
            CALL populate_drug_and_concept(171, '');
            CALL populate_drug_and_concept(2037, '85a6e834-0d60-48d1-8847-c42d2e16e57e'); -- RH (combination Rifampicin+Isoniazid) 150/75mg 225milligram Tablet -> Rifampicin (R) 150mg + Isoniazid (H) 75mg tablet
            CALL populate_drug_and_concept(173, '');
            CALL populate_drug_and_concept(2231, '');
            CALL populate_drug_and_concept(1981, '');
            CALL populate_drug_and_concept(2036, '93b4e4d6-d986-11e5-b5d2-0a1d41d68578'); -- Rifabutin 150milligram Tablet -> Rifabutin, 150mg capsule
            CALL populate_drug_and_concept(2248, '');
            CALL populate_drug_and_concept(22, '78fab944-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Rifampicin 300milligram Tablet -> Rifampicin (R), 300mg capsule
            CALL populate_drug_and_concept(2249, '');
            CALL populate_drug_and_concept(1624, '');
            CALL populate_drug_and_concept(2436, '');
            CALL populate_drug_and_concept(55, '');
            CALL populate_drug_and_concept(177, '');
            CALL populate_drug_and_concept(2232, '');
            CALL populate_drug_and_concept(15, '');
            CALL populate_drug_and_concept(2218, '');
            CALL populate_drug_and_concept(26, 'f55edf29-b1ac-4c43-8fb4-57a9c4415c0c'); -- Streptomycin 1gram Ampulle -> Streptomycin sulfate, Powder for solution for injection, 1g vial
            CALL populate_drug_and_concept(2250, '');
            CALL populate_drug_and_concept(180, '');
            CALL populate_drug_and_concept(40, '');
            CALL populate_drug_and_concept(1617, '78faa576-dfbe-11e9-8a34-2a2ae2dbcce4'); -- Tenofovir 300milligram Tablet -> Tenofovir disoproxil fumarate (TDF), 300mg tablet
            CALL populate_drug_and_concept(2229, '');
            CALL populate_drug_and_concept(1553, '');
            CALL populate_drug_and_concept(1782, '');
            CALL populate_drug_and_concept(51, '');
            CALL populate_drug_and_concept(16, '');
            CALL populate_drug_and_concept(2215, '');
            CALL populate_drug_and_concept(1635, '');
            CALL populate_drug_and_concept(50, '');

            DROP PROCEDURE IF EXISTS populate_drug_and_concept;
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
                    update  hivmigration_drug_orders
                    set     order_reason_non_coded = concat(_source_product_type, concat(' - ', _source_regime_type))
                    where   order_action = 'NEW'
                    and     ((_source_product_type is null and source_product_type is null) or _source_product_type = source_product_type)
                    and     ((_source_regime_type is null and source_regime_type is null) or _source_regime_type = source_regime_type);
                ELSE
                    update  hivmigration_drug_orders
                    set     order_reason = @concept_id
                    where   order_action = 'NEW'
                    and     ((_source_product_type is null and source_product_type is null) or _source_product_type = source_product_type)
                    and     ((_source_regime_type is null and source_regime_type is null) or _source_regime_type = source_regime_type);
                END IF;
            END $$
            DELIMITER ;

            CALL populate_new_order_reason('HIV', null, '138405AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Human immunodeficiency virus (HIV) disease
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
            CREATE PROCEDURE populate_discontinue_order_reason ( _reason varchar(100), _concept_uuid char(36))
            BEGIN
                SET @concept_id = (select concept_id from concept where uuid = _concept_uuid);
                IF @concept_id is null THEN
                    update  hivmigration_drug_orders
                    set     order_reason_non_coded = _reason
                    where   order_action = 'DISCONTINUE'
                    and     source_discontinue_reason = _reason;
                ELSE
                    IF _reason is null THEN
                        update  hivmigration_drug_orders
                        set     order_reason = @concept_id
                        where   order_action = 'DISCONTINUE'
                        and     source_discontinue_reason is null;
                    ELSE
                        update  hivmigration_drug_orders
                        set     order_reason = @concept_id
                        where   order_action = 'DISCONTINUE'
                        and     source_discontinue_reason = _reason;
                    END IF;
                END IF;
            END $$
            DELIMITER ;

            CALL populate_discontinue_order_reason('abandoned', '3cdd5176-26fe-102b-80cb-0017a47871b2'); -- Patient defaulted
            CALL populate_discontinue_order_reason('cd4_augmented', '');
            CALL populate_discontinue_order_reason('compliance', '159598AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Non-compliance with treatment or therapy
            CALL populate_discontinue_order_reason('died', '3cdd446a-26fe-102b-80cb-0017a47871b2'); -- Patient died
            CALL populate_discontinue_order_reason('dose_change', '981AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Dosing change
            CALL populate_discontinue_order_reason('finished_prophylaxis', '');
            CALL populate_discontinue_order_reason('finished_ptme', '3cd91b56-26fe-102b-80cb-0017a47871b2'); -- Completed total PMTCT
            CALL populate_discontinue_order_reason('finished_treatment', '3cdcecea-26fe-102b-80cb-0017a47871b2'); -- Treatment complete
            CALL populate_discontinue_order_reason('formulation_change', '981AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'); -- Dosing change
            CALL populate_discontinue_order_reason('ineffective', '3cd49432-26fe-102b-80cb-0017a47871b2');  -- Regimen failure
            CALL populate_discontinue_order_reason('lost', '3cdd5176-26fe-102b-80cb-0017a47871b2'); -- Patient defaulted
            CALL populate_discontinue_order_reason('moved_in_haiti_txfer', '');
            CALL populate_discontinue_order_reason('moved_out_haiti_txfe', '');
            CALL populate_discontinue_order_reason('other', '3cee7fb4-26fe-102b-80cb-0017a47871b2'); -- Other non-coded
            CALL populate_discontinue_order_reason('pregnancy', '3cdd8132-26fe-102b-80cb-0017a47871b2'); -- Patient pregnant
            CALL populate_discontinue_order_reason('prev_undoc_txfer_con', '');
            CALL populate_discontinue_order_reason('prev_undoc_txfer_not', '');
            CALL populate_discontinue_order_reason('refused_return_to_tr', 'efab937b-853e-47da-b97e-220f1bdff97d'); -- Refusal of treatment by patient
            CALL populate_discontinue_order_reason('resistant', '');
            CALL populate_discontinue_order_reason('side_effect', '3cccecdc-26fe-102b-80cb-0017a47871b2'); -- Toxicity, drug
            CALL populate_discontinue_order_reason('stock_out', '3cde143a-26fe-102b-80cb-0017a47871b2'); -- Medications unavailable
            CALL populate_discontinue_order_reason('tb', '');
            CALL populate_discontinue_order_reason('traced_not_found', '');
            CALL populate_discontinue_order_reason('transferred_out', '3cdd5c02-26fe-102b-80cb-0017a47871b2'); -- Patient transferred out
            CALL populate_discontinue_order_reason('treatment_refused', 'efab937b-853e-47da-b97e-220f1bdff97d'); -- Refusal of treatment by patient
            CALL populate_discontinue_order_reason('treatment_stopped_ot', '3cdc0d7a-26fe-102b-80cb-0017a47871b2'); -- Treatment stopped
            CALL populate_discontinue_order_reason('tx_stop_side_effects', '3cccecdc-26fe-102b-80cb-0017a47871b2'); -- Toxicity, drug
            CALL populate_discontinue_order_reason(null, '3cd743f8-26fe-102b-80cb-0017a47871b2'); -- None

            DROP PROCEDURE IF EXISTS populate_discontinue_order_reason;
        ''')
    }

    void populateDosingInstructions() {
        // TODO: dosing_instructions from ddd, dwd, morning_dose, noon_dose, evening_dose, night_dose
    }

    void createOrders() {
        // TODO
    }

    void createDrugOrders() {
        // TODO
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
    }
}
