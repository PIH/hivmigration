package org.pih.hivmigration.etl.sql

class TbStatusMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        /*
            The TB status table contains observations about a patient's TB status as recorded on an encounter form
            In most cases, this table keys directly on encounter, and in these cases, we can update directly
            In other cases, this table does not store encounter id, and in these cases we try to derive the encounter
            that created it via comparing the hiv_encounters_aud.modified and hiv_encounters.date_created with hiv_tb_status.entered_date

            This migrator migrates in the hiv_tb_status table.
            It depends on data migrated in within the StagingTablesMigrator and EncounterMigrator
        */

        // Load the entire source table from Oracle, and retain it for posterity
        // This is already accomplished in StagingTablesMigrator, which migrates the HIV_TB_STATUS table


        // Query the hiv encounters audit table to see if any of these correspond to follow-up entries, and update derived table if so

        executeMysql("Link follow-up encounter by entry date if found in the encounter table", '''
            update              hivmigration_hiv_status s
            inner join          hivmigration_encounters e 
            on                  s.source_patient_id = e.source_patient_id and date(s.entered_date) = date(e.date_created) and e.source_encounter_type = 'followup' 
            set                 s.derived_followup_id = e.source_encounter_id
            ;
        ''')

        executeMysql("Link follow-up encounter by entry date if found in the audit table", '''
            update              hivmigration_hiv_status s
            inner join          hivmigration_encounters_aud e 
            on                  s.source_patient_id = e.source_patient_id and date(s.entered_date) = date(e.modified) and e.type = 'followup' 
            set                 s.derived_followup_id = e.source_encounter_id
            ;
        ''')

        // Identify the earliest intake encounter associated with each status row (by patient)

        executeMysql("Link intake encounter if found in the encounter table", '''
            update              hivmigration_hiv_status s
            inner join          (select source_patient_id, min(encounter_date) as earliest_intake_date from hivmigration_encounters where source_encounter_type = 'intake' group by source_patient_id) m
            on                  s.source_patient_id = m.source_patient_id
            set                 s.derived_intake_date = m.earliest_intake_date
            ;
        ''')

        executeMysql("Link intake encounter if found in the encounter table", '''
            update              hivmigration_hiv_status s
            inner join          hivmigration_encounters e
            on                  s.source_patient_id = e.source_patient_id and s.derived_intake_date = e.encounter_date and e.source_encounter_type = 'intake'
            set                 s.derived_intake_id = e.source_encounter_id
            ;
        ''')

        executeMysql("Identify the first entry date of a v2 follow-up form for each patient", '''
            update hivmigration_hiv_status set derived_first_v2_followup = null;
            
            update              hivmigration_hiv_status s
            inner join          ( select        min(date_created) as min_date, source_patient_id 
                                  from          hivmigration_encounters 
                                  where         source_encounter_type = 'followup' and form_version = '2' 
                                  group by      source_patient_id
                                ) e
            on                  s.source_patient_id = e.source_patient_id
            set                 s.derived_first_v2_followup = e.min_date
            ;
        ''')

        // If the patient has no v2 follow-up forms in their history, then identify the latest entered status
        // If the patient does have v2 follow-up forms, identify the latest entered status prior to the first v2 followup
        // Indicate the row that should be migrated by populating the derived_target_encounter_id on it

        executeMysql("Use the latest for a patient if they have no v2 followups", '''
            update              hivmigration_hiv_status s
            inner join          ( select    max(hiv_status_id) as max_status_id, source_patient_id
                                  from      hivmigration_hiv_status 
                                  where     (derived_first_v2_followup is null or entered_date < derived_first_v2_followup)
                                  group by  source_patient_id
                                ) m 
            on                  s.hiv_status_id = m.max_status_id 
            set                 s.derived_target_encounter_id = ifnull(s.source_encounter_id, s.derived_intake_id)
            ;
        ''')

        create_tmp_obs_table()

        executeMysql("Load test date observations", ''' 

            SET @test_date_question = concept_uuid_from_mapping('CIEL', '164400'); -- HIV test date
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT  source_patient_id, derived_target_encounter_id, @test_date_question, status_date
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null;
            
        ''')

        executeMysql("Load test result observations", ''' 

            SET @test_result_question = concept_uuid_from_mapping('CIEL', '163722'); -- Rapid test for HIV
            SET @trueValue = concept_uuid_from_mapping('CIEL', '703'); -- Positive
            SET @falseValue = concept_uuid_from_mapping('CIEL', '664'); -- Negative
            SET @unknownValue = concept_uuid_from_mapping('CIEL', '1138'); -- Indeterminate
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_result_question, @trueValue
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     hiv_positive_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_result_question, @falseValue
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     hiv_positive_p = 'f';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_result_question, @unknownValue
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     hiv_positive_p = '?';
            
        ''')

        executeMysql("Load test location observations", ''' 

            SET @test_location_question = concept_uuid_from_mapping('CIEL', '159936'); -- Point of HIV testing
            SET @vct_clinic = concept_uuid_from_mapping('CIEL', '159940'); -- vct
            SET @womens_health_clinic = concept_uuid_from_mapping('CIEL', '159937'); -- MCH
            SET @primary_care_clinic = concept_uuid_from_mapping('CIEL', '160542'); -- Outpatient
            SET @mobile_clinic = concept_uuid_from_mapping('CIEL', '159939'); -- Mobile
            SET @family_planning_clinic = concept_uuid_from_mapping('PIH', '2714'); -- Family planning clinic
            SET @maternity_ward = concept_uuid_from_mapping('PIH', '2235'); -- inpatient maternity
            SET @surgical_ward = concept_uuid_from_mapping('PIH', '6298'); -- surgery
            SET @medicine_ward = concept_uuid_from_mapping('PIH', '7891'); -- inpatient internal med
            SET @pediatric_ward = concept_uuid_from_mapping('PIH', '2717'); -- inpatient peds
            SET @other = concept_uuid_from_mapping('CIEL', '5622'); -- other
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @vct_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'vct_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @womens_health_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'womens_health_clinic';

            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @primary_care_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'primary_care_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @mobile_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'mobile_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @family_planning_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'family_planning_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @maternity_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'maternity_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @surgical_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'surgical_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @medicine_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'medicine_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @pediatric_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'pediatric_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid, comments)
            SELECT  source_patient_id, derived_target_encounter_id, @test_location_question, @other, test_location_other
            FROM    hivmigration_hiv_status
            WHERE   derived_target_encounter_id is not null
            AND     test_location = 'other';                                                                                    
        ''')

        // TODO: We are not currently migrating date_unknown_p.  There are 73 of these in the source system (as of 3/11/21)

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql('''
            SET @test_date_question = concept_from_mapping('CIEL', '164400'); -- HIV test date
            SET @test_result_question = concept_from_mapping('CIEL', '163722'); -- Rapid test for HIV
            SET @test_location_question = concept_from_mapping('CIEL', '159936'); -- Point of HIV testing
            
            delete o.* 
            from obs o 
            inner join hivmigration_encounters e on o.encounter_id = e.encounter_id
            and   e.source_encounter_type = 'intake'
            where o.concept_id in (@test_date_question, @test_result_question, @test_location_question);
        ''')

        executeMysql("drop table if exists hivmigration_hiv_status;");
    }
}
