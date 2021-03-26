package org.pih.hivmigration.etl.sql

class TbStatusMigrator extends ObsMigrator {

    SqlMigrator hivTbStatusMigrator = new TableStager("HIV_TB_STATUS");

    @Override
    def void migrate() {

        /*
            The TB status table contains observations about a patient's TB status as recorded on an encounter form
            In most cases, this table keys directly on encounter, and in these cases, we can update directly
            However, on many of the older intake forms, this did not reference encounter, only patient
            And on some of the follow-up forms (notably the v2), the table was updated with the results of a
            question that did not necessarily denote history at intake, and also did not link to the encounter id.
            When we can't tie a patient's status directly to the intake encounter by foreign key, we
            fall back to looking at overall observations for this patient, and using those as a proxy for the patient's TB history at intake.

            This migrator migrates in the hiv_tb_status table.
            It depends on data migrated in within the EncounterMigrator
        */


        // Load the entire source table from Oracle, and retain it for posterity
        hivTbStatusMigrator.migrate()

        executeMysql("Create stating table containing INTAKE data to migrate", '''
            create table hivmigration_tb_status_intake (
              source_patient_id int,
              source_encounter_id int,
              intake_date date,
              intake_entry_date date,
              status_on_encounter boolean,
              status_date date,
              entered_date date,
              entered_by int,
              ppd_positive_p char(1),
              tb_active_p char(1),
              extrapulmonary_p char(1),
              pulmonary_p char(1),
              drug_resistant_p char(1),
              drug_resistant_comment varchar(1000)
            );
            CREATE INDEX source_patient_id_idx ON hivmigration_tb_status_intake (`source_patient_id`);
            CREATE INDEX source_encounter_id_idx ON hivmigration_tb_status_intake (`source_encounter_id`);
            CREATE INDEX intake_date_idx ON hivmigration_tb_status_intake (`intake_date`);
            CREATE INDEX status_date_idx ON hivmigration_tb_status_intake (`status_date`);
        ''')

        // Initialize a row for all patient intake encounters

        executeMysql("Add empty status row for all patient intakes", '''
            insert into hivmigration_tb_status_intake (source_patient_id, source_encounter_id, intake_date, intake_entry_date, status_on_encounter)
            select  source_patient_id, source_encounter_id, encounter_date, date_created, false
            from    hivmigration_encounters
            where   source_encounter_type = 'intake'
            ;
        ''')

        // Populate row for any patients who have a directly referenced intake encounter

        executeMysql("Load status rows that are directly linked to intake encounters", '''
            update      hivmigration_tb_status_intake i
            inner join  hivmigration_tb_status s on i.source_encounter_id = s.source_encounter_id
            set         i.STATUS_ON_ENCOUNTER = true,
                        i.STATUS_DATE = s.STATUS_DATE, 
                        i.ENTERED_DATE = s.ENTERED_DATE, 
                        i.ENTERED_BY = s.ENTERED_BY,
                        i.PPD_POSITIVE_P = s.PPD_POSITIVE_P, 
                        i.TB_ACTIVE_P = s.TB_ACTIVE_P, 
                        i.EXTRAPULMONARY_P = s.EXTRAPULMONARY_P, 
                        i.PULMONARY_P = s.PULMONARY_P, 
                        i.DRUG_RESISTANT_P = s.DRUG_RESISTANT_P, 
                        i.DRUG_RESISTANT_COMMENT = s.DRUG_RESISTANT_COMMENT
        ''')

        // For the remaining patients, iterate over statuses that are not linked to an encounter, and use to populate

        executeMysql("Add aggregate information to intake where multiple patient-level rows exist", '''
            update        hivmigration_tb_status_intake i
            inner join    (
                            select    s.source_patient_id,
                                      min(s.status_date) as first_status_date,
                                      sum(if(s.ppd_positive_p = 't', 1, 0)) as num_ppd_positive,
                                      sum(if(s.tb_active_p = 't', 1, 0)) as num_tb_active,
                                      sum(if(s.extrapulmonary_p = 't', 1, 0)) as num_extrapulmonary,
                                      sum(if(s.pulmonary_p = 't', 1, 0)) as num_pulmonary,
                                      sum(if(s.drug_resistant_p = 't', 1, 0)) as num_drug_resistant,
                                      group_concat(s.drug_resistant_comment, ' , ') as all_drug_resistant_comment
                            from      hivmigration_tb_status s
                            where     s.source_encounter_id is null
                            group by  s.source_patient_id
                          ) s on i.source_patient_id = s.source_patient_id
            set           i.STATUS_DATE = s.first_status_date, 
                          i.PPD_POSITIVE_P = if(num_ppd_positive > 0, 't', 'f'), 
                          i.TB_ACTIVE_P = if(num_tb_active > 0, 't', 'f'), 
                          i.EXTRAPULMONARY_P = if(num_extrapulmonary > 0, 't', 'f'), 
                          i.PULMONARY_P = if(num_pulmonary > 0, 't', 'f'), 
                          i.DRUG_RESISTANT_P = if(num_drug_resistant > 0, 't', 'f'), 
                          i.DRUG_RESISTANT_COMMENT = s.all_drug_resistant_comment
            where         i.status_on_encounter = false
            ;
        ''')

        // Update rows based on values entered by hop_abstraction encounters

        executeMysql("Set derived observation values based on hop abstraction if it exists", '''
            update              hivmigration_tb_status_intake i
            inner join          (
                                    select  source_patient_id, status_date, extrapulmonary_p, drug_resistant_p 
                                    from    hivmigration_tb_status
                                    where   source_encounter_id in (277018, 277029, 287844, 341199)
                                ) s on i.source_patient_id = s.source_patient_id
            set                 i.status_date = s.status_date,
                                i.extrapulmonary_p = s.extrapulmonary_p,
                                i.drug_resistant_p = s.drug_resistant_p
            ;
        ''')

        create_tmp_obs_table()

        // Moved / Adapted from original inclusion in ExamExtraMigrator, following same pattern from HivStatusMigrator

        executeMysql("Load test date observations", ''' 

            SET @test_date_question = concept_uuid_from_mapping('PIH', '11526'); -- Date of tuberculosis test
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_datetime)
            SELECT  source_patient_id, source_encounter_id, @test_date_question, status_date
            FROM    hivmigration_tb_status_intake
            WHERE   status_date is not null;
            
        ''')

        // Only migrate in the 't' values, as the default for the column is 'f', making entries unreliable

        executeMysql("Load tb active history observations", ''' 

            SET @test_result_question = concept_uuid_from_mapping('CIEL', '1389'); -- History of Tuberculosis
            SET @trueValue = concept_uuid_from_mapping('CIEL', '1065'); -- True
            SET @falseValue = concept_uuid_from_mapping('CIEL', '1066'); -- False
            SET @unknownValue = concept_uuid_from_mapping('CIEL', '1067'); -- Unknown
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @test_result_question, @trueValue
            FROM    hivmigration_tb_status_intake
            WHERE   tb_active_p = 't';

        ''')

        executeMysql("Load site of tb disease observations", ''' 

            SET @tb_location = concept_uuid_from_mapping('CIEL', '160040'); -- Location of TB disease
            SET @pulmonary = concept_uuid_from_mapping('CIEL', '42'); -- Pulmonary Tuberculosis
            SET @extrapulmonary = concept_uuid_from_mapping('CIEL', '5042'); -- Extra-pulmonary tuberculosis
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @tb_location, @pulmonary
            FROM    hivmigration_tb_status_intake
            WHERE   pulmonary_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @tb_location, @extrapulmonary
            FROM    hivmigration_tb_status_intake
            WHERE   extrapulmonary_p = 't';
            
        ''')

        executeMysql("Load history of resistant tb observations", ''' 

            SET @dst_complete = concept_uuid_from_mapping('PIH', '3039'); -- Drug sensitivity test complete
            SET @dst_results = concept_uuid_from_mapping('CIEL', '159391 '); -- Tuberculosis drug sensitivity testing (text)
            SET @trueValue = concept_uuid_from_mapping('CIEL', '1065'); -- True
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @dst_complete, @trueValue
            FROM    hivmigration_tb_status_intake
            WHERE   drug_resistant_p = 't';
            
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_text)
            SELECT  source_patient_id, source_encounter_id, @dst_results, drug_resistant_comment
            FROM    hivmigration_tb_status_intake
            WHERE   drug_resistant_comment is not null and drug_resistant_comment != '';
            
        ''')

        executeMysql("Load ppd positive observation", ''' 

            SET @ppd_result = concept_uuid_from_mapping('PIH', '1435'); -- PPD, qualitative
            SET @positive = concept_uuid_from_mapping('CIEL', '703'); -- Positive
                        
            INSERT INTO tmp_obs (source_patient_id, source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT  source_patient_id, source_encounter_id, @ppd_result, @positive
            FROM    hivmigration_tb_status_intake
            WHERE   ppd_positive_p = 't';
            
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        if (tableExists("hivmigration_tb_status_intake")) {
            executeMysql('''
                SET @status_date = concept_from_mapping('PIH', '11526'); -- Date of tuberculosis test
                SET @tb_active = concept_from_mapping('CIEL', '1389'); -- History of Tuberculosis
                SET @tb_location = concept_from_mapping('CIEL', '160040'); -- Location of TB disease
                SET @dst_complete = concept_from_mapping('PIH', '3039'); -- Drug sensitivity test complete
                SET @dst_results = concept_from_mapping('CIEL', '159391 '); -- Tuberculosis drug sensitivity testing (text)
                SET @ppd_result = concept_from_mapping('PIH', '1435'); -- PPD, qualitative
                
                delete o.* 
                from obs o 
                inner join hivmigration_encounters e on o.encounter_id = e.encounter_id
                inner join hivmigration_tb_status_intake s on e.source_encounter_id = s.source_encounter_id
                where o.concept_id in (@status_date, @tb_active, @tb_location, @dst_complete, @dst_results, @ppd_result);
            ''')

            executeMysql("drop table hivmigration_tb_status_intake")
        }

        hivTbStatusMigrator.revert()
    }
}
