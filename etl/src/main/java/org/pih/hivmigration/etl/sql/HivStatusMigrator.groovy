package org.pih.hivmigration.etl.sql

class HivStatusMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        /*
            See overall analysis here:  https://pihemr.atlassian.net/wiki/spaces/ZL/pages/1230045211/HIV+HIV+STATUS

            The HIV_HIV_STATUS table is populated in the following 3 ways:

            1. On Intake v1:  Row created in hiv_hiv_status, no encounter id linked
            2. On Intake v2 and v3:  Row created in hiv_hiv_status, intake encounter id linked
            3. Followup v2:  Row created in hiv_hiv_status with entry date of form if status is changed from pre-populated value, no encounter id linked

            Each of these represents an insert into hiv_hiv_status, which means that we have the full audit trail of this data and these changes

            #3 is populated in response to the question:  HIV/TB Status, with values "HIV Only", "TB Only", "HIV/TB Co-infected".
            This is different from the way it is used for #1 and #2, which ask further questions around Test Date, Test Result, Test Location, etc.
            So this is really different data that was incorrectly put into this table from the v2 followup.

            However, since all patients within this system (particularly those with a Follow-up Form entered) are HIV positive,
            this is also unnecessary data to retain.  An observation that says that a patient is HIV positive as of the entry date
            of an HIV followup form is not useful.

            Because of this, and because of the fact that every patient is expected to have a single answer to this question,
            which is captured only on their intake form, this migration will do the following:

            1. Attempt to exclude all entries from the hiv_hiv_status table that were recorded as a result of follow-up data entry.
               This will ensure that follow-up form entry dates are not interpreted as the hiv diagnosis date for a patient
            2. Re-calculate the "current" value from this table based on the entry date, to get the "latest" value for each patient
               This will attempt to get the most recently entered value, in order to account for corrections, treating most recent as most accurate.
               If a follow-up v2 encounter is detected, then treat the latest value prior ot this as the most recent
            3. Migrate obs for each patient based on this most recently entered value
            4. Create data warnings as useful

            Dependencies and mappings:

            This migrator migrates in the hiv_hiv_status table.
            It depends on data within hiv_encounters (EncounterMigrator) and hiv_encounters_aud (StagingTablesMigrator)
        */

        // Load the entire source table from Oracle, and retain it for posterity

        executeMysql("Create staging table for HIV_STATUS", '''
            create table hivmigration_hiv_status (
              hiv_status_id int,
              source_encounter_id int,
              source_patient_id int,
              status_date date,
              date_unknown_p char(1),
              entered_date date,
              entered_by int,
              type varchar(10),
              hiv_positive_p char(1),
              test_location varchar(200),
              test_location_other varchar(200),
              
              derived_followup_id int,
              derived_intake_date date,
              derived_intake_id int,
              derived_first_v2_followup date,
              derived_migrate_row boolean
            );
            CREATE INDEX source_encounter_id_idx ON hivmigration_hiv_status (`source_encounter_id`);
            CREATE INDEX source_patient_id_idx ON hivmigration_hiv_status (`source_patient_id`);
            CREATE INDEX status_date_idx ON hivmigration_hiv_status (`status_date`);
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_hiv_status (
              hiv_status_id,
              source_encounter_id,
              source_patient_id,
              status_date,
              date_unknown_p,
              entered_date,
              entered_by,
              type,
              hiv_positive_p,
              test_location,
              test_location_other
            )
            values(?,?,?,?,?,?,?,?,?,?,?) 
            ''', '''
            select 
                s.HIV_STATUS_ID,
                s.ENCOUNTER_ID,
                s.PATIENT_ID,
                s.STATUS_DATE,
                s.DATE_UNKNOWN_P,
                s.ENTERED_DATE,
                s.ENTERED_BY,
                s.TYPE,
                s.HIV_POSITIVE_P,
                s.TEST_LOCATION,
                s.TEST_LOCATION_OTHER
            from 
                HIV_HIV_STATUS s, HIV_DEMOGRAPHICS_REAL d  
            where 
                s.PATIENT_ID = d.PATIENT_ID;
        ''')

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
            on                  s.source_patient_id = e.source_patient_id and s.derived_intake_date = e.encounter_date
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

        executeMysql("Use the latest for a patient if they have no v2 followups", '''
            update              hivmigration_hiv_status s
            inner join          ( select    max(hiv_status_id) as max_status_id, source_patient_id
                                  from      hivmigration_hiv_status 
                                  where     (derived_first_v2_followup is null or entered_date < derived_first_v2_followup)
                                  group by  source_patient_id
                                ) m 
            on                  s.hiv_status_id = m.max_status_id 
            set                 s.derived_migrate_row = true
            ;
        ''')

        create_tmp_obs_table()

        executeMysql("Load test date observations", ''' 

            SET @test_date_question = concept_uuid_from_mapping('CIEL', '164400'); -- HIV test date
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT  source_patient_id, source_encounter_id, @test_date_question, status_date
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true;
            
        ''')

        executeMysql("Load test result observations", ''' 

            SET @test_result_question = concept_uuid_from_mapping('CIEL', '163722'); -- Rapid test for HIV
            SET @trueValue = concept_uuid_from_mapping('CIEL', '703'); -- Positive
            SET @falseValue = concept_uuid_from_mapping('CIEL', '664'); -- Negative
            SET @unknownValue = concept_uuid_from_mapping('CIEL', '1138'); -- Indeterminate
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_result_question, @trueValue
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     hiv_positive_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_result_question, @falseValue
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     hiv_positive_p = 'f';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_result_question, @unknownValue
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
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
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @vct_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'vct_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @womens_health_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'womens_health_clinic';

            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @primary_care_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'primary_care_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @mobile_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'mobile_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @family_planning_clinic
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'family_planning_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @maternity_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'maternity_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @surgical_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'surgical_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @medicine_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'medicine_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @pediatric_ward
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'pediatric_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid, comments)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @other, test_location_other
            FROM    hivmigration_hiv_status
            WHERE   derived_migrate_row = true
            AND     test_location = 'other';                                                                                    
        ''')

        // TODO: We are not currently migrating date_unknown_p.  There are 73 of these in the source system (as of 3/11/21)

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_hiv_status;");
    }
}
