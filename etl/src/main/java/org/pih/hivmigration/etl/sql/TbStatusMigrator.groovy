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

        executeMysql("Create staging table for HIV_TB_STATUS", '''
            create table hivmigration_tb_status (
              tb_status_id int,
              source_encounter_id int,
              source_patient_id int,
              source_encounter_type varchar(20),
              status_date date,
              entered_date date,
              entered_by int,
              type varchar(10),
              ppd_positive_p char(1),
              tb_active_p char(1),
              extrapulmonary_p char(1),
              pulmonary_p char(1),
              drug_resistant_p char(1),
              drug_resistant_comment varchar(1000),

              derived_followup_id int,
              derived_intake_date date,
              derived_intake_id int,
              derived_first_v2_followup date,
              derived_target_encounter_id int,
              derived_status_date date,
              derived_extrapulmonary_p char(1),
              derived_drug_resistant_p char(1)
            );
            CREATE INDEX source_encounter_id_idx ON hivmigration_tb_status (`source_encounter_id`);
            CREATE INDEX source_patient_id_idx ON hivmigration_tb_status (`source_patient_id`);
            CREATE INDEX status_date_idx ON hivmigration_tb_status (`status_date`);
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_tb_status (
              tb_status_id,
              source_encounter_id,
              source_patient_id,
              source_encounter_type,
              status_date,
              entered_date,
              entered_by,
              type,
              ppd_positive_p,
              tb_active_p,
              extrapulmonary_p,
              pulmonary_p,
              drug_resistant_p,
              drug_resistant_comment
            )
            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
            ''', '''
            select 
                s.TB_STATUS_ID,
                s.ENCOUNTER_ID,
                s.PATIENT_ID,
                e.TYPE,
                s.STATUS_DATE,
                s.ENTERED_DATE,
                s.ENTERED_BY,
                s.TYPE,
                s.PPD_POSITIVE_P,
                s.TB_ACTIVE_P,
                s.EXTRAPULMONARY_P,
                s.PULMONARY_P,
                s.DRUG_RESISTANT_P,
                s.DRUG_RESISTANT_COMMENT
            from 
                HIV_TB_STATUS s, HIV_DEMOGRAPHICS_REAL d, HIV_ENCOUNTERS e
            where 
                s.PATIENT_ID = d.PATIENT_ID
            and s.ENCOUNTER_ID = e.ENCOUNTER_ID(+);
        ''')

        // NOTE:  THIS IS THE SAME APPROACH TAKEN IN THE HivStatusMigrator.  WE'LL WANT TO CONSIDER BOTH WHEN MAKING CHANGES.

        // Query the hiv encounters audit table to see if any of these correspond to follow-up entries, and update derived table if so

        executeMysql("Link follow-up encounter by entry date if found in the encounter table", '''
            update              hivmigration_tb_status s
            inner join          hivmigration_encounters e 
            on                  s.source_patient_id = e.source_patient_id and date(s.entered_date) = date(e.date_created) and e.source_encounter_type = 'followup' 
            set                 s.derived_followup_id = e.source_encounter_id
            ;
        ''')

        executeMysql("Link follow-up encounter by entry date if found in the audit table", '''
            update              hivmigration_tb_status s
            inner join          hivmigration_encounters_aud e 
            on                  s.source_patient_id = e.source_patient_id and date(s.entered_date) = date(e.modified) and e.type = 'followup' 
            set                 s.derived_followup_id = e.source_encounter_id
            ;
        ''')

        // Identify the earliest intake encounter associated with each status row (by patient)

        executeMysql("Link intake encounter if found in the encounter table", '''
            update              hivmigration_tb_status s
            inner join          (select source_patient_id, min(encounter_date) as earliest_intake_date from hivmigration_encounters where source_encounter_type = 'intake' group by source_patient_id) m
            on                  s.source_patient_id = m.source_patient_id
            set                 s.derived_intake_date = m.earliest_intake_date
            ;
        ''')

        executeMysql("Link intake encounter if found in the encounter table", '''
            update              hivmigration_tb_status s
            inner join          hivmigration_encounters e
            on                  s.source_patient_id = e.source_patient_id and s.derived_intake_date = e.encounter_date and e.source_encounter_type = 'intake'
            set                 s.derived_intake_id = e.source_encounter_id
            ;
        ''')

        executeMysql("Identify the first entry date of a v2 follow-up form for each patient", '''
            update hivmigration_tb_status set derived_first_v2_followup = null;
            
            update              hivmigration_tb_status s
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
            update              hivmigration_tb_status s
            inner join          ( select    max(tb_status_id) as max_status_id, source_patient_id
                                  from      hivmigration_tb_status 
                                  where     (derived_first_v2_followup is null or entered_date < derived_first_v2_followup)
                                  group by  source_patient_id
                                ) m 
            on                  s.tb_status_id = m.max_status_id 
            set                 s.derived_target_encounter_id = if(s.source_encounter_id is null or s.source_encounter_type = 'hop_abstraction', s.derived_intake_id, s.source_encounter_id)
            ;
        ''')

        executeMysql("Set derived observation values based on hop abstraction if it exists", '''
            update              hivmigration_tb_status s
            left join           (select source_patient_id, status_date, extrapulmonary_p, drug_resistant_p from hivmigration_tb_status s2 group by source_patient_id) hop
            on                  s.source_patient_id = hop.source_patient_id
            set                 s.derived_status_date = ifnull(hop.status_date, s.status_date),
                                s.derived_extrapulmonary_p = ifnull(hop.extrapulmonary_p, s.extrapulmonary_p),
                                s.derived_drug_resistant_p = ifnull(hop.drug_resistant_p, s.drug_resistant_p)
            ;
        ''')

        create_tmp_obs_table()

        // Moved / Adapted from original inclusion in ExamExtraMigrator, following same pattern from HivStatusMigrator

        executeMysql("Load test date observations", ''' 

            SET @test_date_question = concept_uuid_from_mapping('PIH', '11526'); -- Date of tuberculosis test
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT  source_patient_id, derived_target_encounter_id, @test_date_question, derived_status_date
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     derived_status_date is not null;
            
        ''')

        executeMysql("Load tb active history observations", ''' 

            SET @test_result_question = concept_uuid_from_mapping('CIEL', '1389'); -- History of Tuberculosis
            SET @trueValue = concept_uuid_from_mapping('CIEL', '1065'); -- True
            SET @falseValue = concept_uuid_from_mapping('CIEL', '1066'); -- False
            SET @unknownValue = concept_uuid_from_mapping('CIEL', '1067'); -- Unknown
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_result_question, @trueValue
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     tb_active_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_result_question, @falseValue
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     tb_active_p = 'f';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @test_result_question, @unknownValue
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     tb_active_p = '?';
            
        ''')

        executeMysql("Load site of tb disease observations", ''' 

            SET @tb_location = concept_uuid_from_mapping('CIEL', '160040'); -- Location of TB disease
            SET @pulmonary = concept_uuid_from_mapping('CIEL', '42'); -- Pulmonary Tuberculosis
            SET @extrapulmonary = concept_uuid_from_mapping('CIEL', '5042'); -- Extra-pulmonary tuberculosis
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @tb_location, @pulmonary
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     pulmonary_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @tb_location, @extrapulmonary
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     derived_extrapulmonary_p = 't';
            
        ''')

        executeMysql("Load history of resistant tb observations", ''' 

            SET @dst_complete = concept_uuid_from_mapping('PIH', '3039'); -- Drug sensitivity test complete
            SET @dst_results = concept_uuid_from_mapping('CIEL', '159391 '); -- Tuberculosis drug sensitivity testing (text)
            SET @trueValue = concept_uuid_from_mapping('CIEL', '1065'); -- True
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @dst_complete, @trueValue
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     derived_drug_resistant_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_text)
            SELECT  source_patient_id, derived_target_encounter_id, @dst_results, drug_resistant_comment
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     drug_resistant_comment is not null;
            
        ''')

        executeMysql("Load ppd positive observation", ''' 

            SET @ppd_result = concept_uuid_from_mapping('PIH', '1435'); -- PPD, qualitative
            SET @positive = concept_uuid_from_mapping('CIEL', '703'); -- Positive
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, derived_target_encounter_id, @ppd_result, @positive
            FROM    hivmigration_tb_status
            WHERE   derived_target_encounter_id is not null
            AND     ppd_positive_p = 't';
            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql('''
            SET @status_date = concept_from_mapping('PIH', '11526'); -- Date of tuberculosis test
            SET @tb_active = concept_from_mapping('CIEL', '1389'); -- History of Tuberculosis
            SET @tb_pulm_extra = concept_from_mapping('CIEL', '160040'); -- Location of TB disease
            SET @dst_complete = concept_from_mapping('PIH', '3039'); -- Drug sensitivity test complete
            SET @dst_results = concept_from_mapping('CIEL', '159391 '); -- Tuberculosis drug sensitivity testing (text)
            SET @ppd_result = concept_from_mapping('PIH', '1435'); -- PPD, qualitative
            
            delete o.* 
            from obs o 
            inner join hivmigration_encounters e on o.encounter_id = e.encounter_id
            inner join hivmigration_tb_status s on e.source_encounter_id = s.derived_target_encounter_id
            where o.concept_id in (@status_date, @tb_active, @tb_pulm_extra, @dst_complete, @dst_results, @ppd_result);
        ''')

        executeMysql("drop table if exists hivmigration_tb_status;")
    }
}
