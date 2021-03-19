package org.pih.hivmigration.etl.sql

class HivStatusMigrator extends ObsMigrator {

    SqlMigrator hivStatusMigrator = new TableStager("HIV_HIV_STATUS")

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
            This is different from the way it phrased/collected for #1 and #2, which ask further questions around Test Date, Test Result, Test Location, etc.

            However, since all patients within this system (particularly those with a Follow-up Form entered) are HIV positive,
            this is also unnecessary data to retain.  An observation that says that a patient is HIV positive as of the entry date
            of an HIV followup form is not useful.

            Because of this, and because of the fact that every patient is expected to have a single answer to this question,
            which is captured only on their intake form, this migration will do the following:

            1. Look at all intake encounters
            2. If a given intake encounter has a foreign keyed status linked to it, migrate this status row onto the intake as-is
            3. If a given intake encounter does _not_ have a status linked to it, try to determine if there are status obs that should be recorded on it
              - There are 4 data points:  status_date, hiv_positive_p, test_location, test_location_other
              - test_location and test_location_other:
                 - These are _only_ ever recorded on intake forms.
                 - So if we find any of these for a patient, record the most recently entered value for each on their intake
              - hiv_positive_p and status_date
                 - These can be recorded either on intake encounters or follow-up encounters.
                 - To determine the appropriate value _at intake_ we do the following:
                    - Find the earliest status date (using entry date if null) for a patient where hiv_positive_p = 't'
                    - If this date is on or before the intake encounter date, use this status_date and hiv_positive_p = 't' for the patient
                    - Find the earliest status date (using entry date if null) for a patient where hiv_positive_p = 'f'
                    - If this date is on or before the intake encounter date _and_ they didn't already have hiv_positive_p = 't' recorded, use this status_date and hiv_positive_p = 'f' for the patient
                    - Find the earliest status date (using entry date if null) for a patient where hiv_positive_p = '?'
                    - If this date is on or before the intake encounter date _and_ they didn't already have hiv_positive_p = 't' or 'f' recorded, use this status_date and hiv_positive_p = '?' for the patient

            Ultimately, the goal is to associate the best status row representing the hiv status at the time of intake for each patient.

            Dependencies and mappings:

            This migrator migrates in the hiv_hiv_status table.
            It depends on data within hiv_encounters (EncounterMigrator)
        */

        // Load the entire source table from Oracle, and retain it for posterity
        hivStatusMigrator.migrate()

        executeMysql("Create stating table containing INTAKE data to migrate", '''
            create table hivmigration_hiv_status_intake (
              source_patient_id int,
              source_encounter_id int,
              intake_date date,
              intake_entry_date date,
              status_on_encounter boolean,
              status_date date,
              entered_date date,
              entered_by int,
              hiv_positive_p char(1),
              test_location varchar(200),
              test_location_other varchar(200)
            );
            CREATE INDEX source_encounter_id_idx ON hivmigration_hiv_status_intake (`source_encounter_id`);
            CREATE INDEX source_patient_id_idx ON hivmigration_hiv_status_intake (`source_patient_id`);
            CREATE INDEX intake_date_idx ON hivmigration_hiv_status_intake (`intake_date`);
            CREATE INDEX status_date_idx ON hivmigration_hiv_status_intake (`status_date`);
        ''')

        // Initialize a row for all patient intake encounters

        executeMysql("Add empty status row for all patient intakes", '''
            insert into hivmigration_hiv_status_intake (source_patient_id, source_encounter_id, intake_date, intake_entry_date, status_on_encounter)
            select  source_patient_id, source_encounter_id, encounter_date, date_created, false
            from    hivmigration_encounters
            where   source_encounter_type = 'intake'
            ;
        ''')

        // Populate row for any patients who have a directly referenced intake encounter

        executeMysql("Load status rows that are directly linked to intake encounters", '''
            update      hivmigration_hiv_status_intake i
            inner join  hivmigration_hiv_status s on i.source_encounter_id = s.source_encounter_id
            set         i.STATUS_ON_ENCOUNTER = true,
                        i.STATUS_DATE = s.STATUS_DATE, 
                        i.ENTERED_DATE = s.ENTERED_DATE, 
                        i.ENTERED_BY = s.ENTERED_BY,
                        i.HIV_POSITIVE_P = s.HIV_POSITIVE_P, 
                        i.TEST_LOCATION = s.TEST_LOCATION, 
                        i.TEST_LOCATION_OTHER = s.TEST_LOCATION_OTHER
        ''')

        // Test location / test location other are only ever collected on intake forms
        // Add those to the intake row for a patient if they are found at all not associated with an encounter

        executeMysql("Add test location and test location other obs to any patients without populated encounter data", '''
            update        hivmigration_hiv_status_intake i
            inner join    (
                            select    source_patient_id,
                                      test_location,
                                      test_location_other
                            from      hivmigration_hiv_status
                            where     test_location is not null or test_location_other is not null
                            and       source_encounter_id is null
                            order by  entered_date asc
                          )
                          s on i.source_patient_id = s.source_patient_id
            set           i.test_location = s.test_location,
                          i.test_location_other = s.test_location_other
            where         i.status_on_encounter = false       
            ;
        ''')

        // If a positive status is indicated prior to intake, record that

        executeMysql("Set hiv_positive_p and hiv_status_date with the earliest positive value prior to intake", '''
            update        hivmigration_hiv_status_intake i
            inner join    (
                            select    source_patient_id, date(min(ifnull(status_date, entered_date))) as earliest_hiv_date
                            from      hivmigration_hiv_status
                            where     hiv_positive_p = 't'
                            group by  source_patient_id
                          )
                          s on i.source_patient_id = s.source_patient_id
            set           i.hiv_positive_p = 't', i.status_date = s.earliest_hiv_date
            where         i.status_on_encounter = false
            and           s.earliest_hiv_date <= i.intake_date
            ;
        ''')

        // If no positive status was indicated prior to intake, but a negative status was, record that

        executeMysql("If hiv_positive_p and status date are null, look at negative values", '''
            update        hivmigration_hiv_status_intake i
            inner join    (
                            select    source_patient_id, date(min(ifnull(status_date, entered_date))) as earliest_non_hiv_date
                            from      hivmigration_hiv_status
                            where     hiv_positive_p in ('f')
                            group by  source_patient_id
                          ) s on i.source_patient_id = s.source_patient_id
            set           i.hiv_positive_p = 'f', i.status_date = s.earliest_non_hiv_date
            where         i.status_on_encounter = false
            and           i.hiv_positive_p is null
            and           s.earliest_non_hiv_date <= i.intake_date
            ;
        ''')

        // If no positive or negative status was indicated prior to intake, but an unknown status was, record that

        executeMysql("If hiv_positive_p and status date are null, look at negative values", '''
            update        hivmigration_hiv_status_intake i
            inner join    (
                            select    source_patient_id, date(min(ifnull(status_date, entered_date))) as earliest_unknown_hiv_date
                            from      hivmigration_hiv_status
                            where     hiv_positive_p in ('?')
                            group by  source_patient_id
                          ) s on i.source_patient_id = s.source_patient_id
            set           i.hiv_positive_p = '?', i.status_date = s.earliest_unknown_hiv_date
            where         i.status_on_encounter = false
            and           i.hiv_positive_p is null
            and           s.earliest_unknown_hiv_date <= i.intake_date
            ;
        ''')

        create_tmp_obs_table()

        executeMysql("Load test date observations", ''' 

            SET @test_date_question = concept_uuid_from_mapping('CIEL', '164400'); -- HIV test date
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT  source_patient_id, source_encounter_id, @test_date_question, status_date
            FROM    hivmigration_hiv_status_intake
            WHERE   status_date is not null;
            
        ''')

        // We choose CIEL:1169 as the question to migrate the HIV Status into, as the v1 intake also collects HIV Rapid Test result

        executeMysql("Load test result observations", ''' 

            SET @status_question = concept_uuid_from_mapping('CIEL', '1169'); -- HIV INFECTED
            SET @trueValue = concept_uuid_from_mapping('CIEL', '703'); -- Positive
            SET @falseValue = concept_uuid_from_mapping('CIEL', '664'); -- Negative
            SET @unknownValue = concept_uuid_from_mapping('CIEL', '1138'); -- Indeterminate
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @status_question, @trueValue
            FROM    hivmigration_hiv_status_intake
            WHERE   hiv_positive_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @status_question, @falseValue
            FROM    hivmigration_hiv_status_intake
            WHERE   hiv_positive_p = 'f';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @status_question, @unknownValue
            FROM    hivmigration_hiv_status_intake
            WHERE   hiv_positive_p = '?';
            
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
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'vct_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @womens_health_clinic
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'womens_health_clinic';

            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @primary_care_clinic
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'primary_care_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @mobile_clinic
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'mobile_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @family_planning_clinic
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'family_planning_clinic';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @maternity_ward
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'maternity_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @surgical_ward
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'surgical_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @medicine_ward
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'medicine_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @pediatric_ward
            FROM    hivmigration_hiv_status_intake
            WHERE   test_location = 'pediatric_ward';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid, comments)
            SELECT  source_patient_id, source_encounter_id, @test_location_question, @other, test_location_other
            FROM    hivmigration_hiv_status_intake
            WHERE   (test_location = 'other' or test_location_other is not null);                                                                                    
        ''')

        // NOTE: We are not currently migrating date_unknown_p.  There are 73 of these in the system (as of 3/11/21)

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql('''
            SET @test_date_question = concept_from_mapping('CIEL', '164400'); -- HIV test date
            SET @status_question = concept_from_mapping('CIEL', '1169'); -- HIV INFECTED
            SET @test_location_question = concept_from_mapping('CIEL', '159936'); -- Point of HIV testing

            delete o.* 
            from obs o 
            inner join hivmigration_encounters e on o.encounter_id = e.encounter_id
            inner join hivmigration_hiv_status_intake s on e.source_encounter_id = s.source_encounter_id
            where o.concept_id in (@status_question, @test_result_question, @test_location_question);
        ''')

        executeMysql("drop table if exists hivmigration_hiv_status_intake")

        hivStatusMigrator.revert()
    }
}
