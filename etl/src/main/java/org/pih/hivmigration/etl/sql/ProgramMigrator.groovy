package org.pih.hivmigration.etl.sql

class ProgramMigrator extends SqlMigrator {

    void migrate() {
        executeMysql("Create staging table to receive program data", '''
            create table hivmigration_programs_raw (
                # these columns receive the data directly from HIV EMR during the "load" step
                source_patient_id int PRIMARY KEY,
                starting_health_center int,
                health_center int,
                health_center_transfer_date date,
                treatment_status varchar(30),
                treatment_status_date date,
                art_start_date date,
                regimen_outcome varchar(20),
                regimen_outcome_date date,
                min_visit_date date,
                max_visit_date date,
                # these columns are calculated afterward
                enrollment_date date,
                outcome_date date,
                outcome varchar(255),
                openmrs_treatment_status int
            );
        ''')

        // One row of input data can be split into two rows of output data. The input data
        // is row-per-patient while the output data is row-per-enrollment.
        // There are transformations we need to make both before and after splitting.
        // Therefore we create another table which holds the split data for further processing.
        executeMysql("Create intermediate staging table", '''
            create table hivmigration_programs (
                patient_program_id int PRIMARY KEY AUTO_INCREMENT,
                source_patient_id int,
                location_id int,
                enrollment_date date,
                art_start_date date,
                outcome_date date,
                outcome varchar(255),
                treatment_status int
            );
        ''')

        setAutoIncrement("patient_program", "(select max(patient_program_id)+1 from patient_program)")

        // Load the data in. Note especially the list of encounter types.
        loadFromOracleToMySql('''
            insert into hivmigration_programs_raw
                (source_patient_id,
                starting_health_center,
                health_center,
                health_center_transfer_date,
                treatment_status,
                treatment_status_date,
                art_start_date,
                regimen_outcome,
                regimen_outcome_date,
                min_visit_date,
                max_visit_date)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', '''
            select 	
                d.patient_id as source_patient_id,
                d.starting_health_center,
                d.health_center,
                d.health_center_transfer_date,
                d.treatment_status,
                d.treatment_status_date,
                r.start_date,
                o.regimen_outcome,
                o.regimen_outcome_date,
                e.min_visit_date,
                e.max_visit_date
            from
                hiv_demographics_real d, hiv_art_regimen_start_date r, hiv_reg_outcome_helper_dates o,
                (
                    select 		patient_id, min(encounter_date) as min_visit_date, max(encounter_date) as max_visit_date 
                    from 		hiv_encounters 
                    where 		type in ('intake','followup','patient_contact','accompagnateur') 
                    and			encounter_date is not null
                    group by 	patient_id
                ) e
            where 
                d.patient_id = e.patient_id
            and
                d.patient_id = r.patient_id(+)
            and
                d.patient_id = o.patient_id(+)
        ''')

        executeMysql("Add enrollment_date and outcome", '''
            SET @outcome_died = concept_from_mapping('PIH', 'PATIENT DIED'); 
            SET @outcome_prev_undoc_txfer_conf = (select concept_id from concept where uuid = '0a5e870e-271c-4a0e-984c-78bd8529f7e9');
            SET @outcome_prev_undoc_txfer_not_conf = (select concept_id from concept where uuid = '977db337-88e8-44f0-878c-7ac973ed0f9e');            
            SET @outcome_moved_in_haiti_txfer_conf = (select concept_id from concept where uuid = '8c1b5627-5f93-4879-83e9-b591aa398148');
            SET @outcome_moved_in_haiti_txfer_not_conf = (select concept_id from concept where uuid = '0ca1975b-d6cf-4ef3-8038-3aa5c4e3178a');
            SET @outcome_moved_out_haiti_txfer_conf = (select concept_id from concept where uuid = 'c62ae7ac-e0de-42e6-a1d6-0a415e0ffdaa');
            SET @outcome_moved_out_haiti_txfer_not_conf = (select concept_id from concept where uuid = '7c8aaf44-890b-43af-8e0e-776aef082998');
            SET @outcome_transfer_to_another_ZL_site = (select concept_id from concept where uuid = '3c392f4d-9fdd-44ad-99db-d2bad176f974');
                       
            UPDATE hivmigration_programs_raw
            SET
                # Enrollment Date in program is earlier of minimum encounter and art start date
                enrollment_date = LEAST (IFNULL(min_visit_date, art_start_date), IFNULL(art_start_date, min_visit_date)),                
                outcome = CASE treatment_status
                    WHEN 'died' THEN @outcome_died
                    WHEN 'lost' THEN null
                    WHEN 'abandoned' THEN null
                    WHEN 'transferred_out' THEN @outcome_transfer_to_another_ZL_site
                    WHEN 'treatment_refused' THEN null                      
                    WHEN 'treatment_stopped_side_effects' THEN null
                    WHEN 'treatment_stopped_other' THEN null 
                    WHEN 'moved_in_haiti_txfer_conf' THEN @outcome_moved_in_haiti_txfer_conf 
                    WHEN 'moved_in_haiti_txfer_not_conf' THEN @outcome_moved_in_haiti_txfer_not_conf
                    WHEN 'moved_out_haiti_txfer_conf' THEN @outcome_moved_out_haiti_txfer_conf 
                    WHEN 'moved_out_haiti_txfer_not_conf' THEN @outcome_moved_out_haiti_txfer_not_conf  
                    WHEN 'prev_undoc_txfer_conf' THEN @outcome_prev_undoc_txfer_conf  
                    WHEN 'prev_undoc_txfer_not_conf' THEN @outcome_prev_undoc_txfer_not_conf 
                    WHEN 'refused_return_to_treatment' THEN null
                    WHEN 'traced_not_found' THEN null
                    ELSE null 
                    END;

            SET @status_lost_to_followup = concept_from_mapping('PIH', 'LOST TO FOLLOWUP');
            SET @status_refused_treatment = concept_from_mapping('PIH', 'TREATMENT NEVER STARTED - PATIENT REFUSED');
            SET @status_stopped_side_effects = concept_from_mapping('PIH', 'TREATMENT STOPPED - SIDE EFFECTS');
            SET @status_stopped_others = concept_from_mapping('PIH', 'Other treatment stopped');                        
                                                
            UPDATE hivmigration_programs_raw
            SET                                
                openmrs_treatment_status = CASE treatment_status
                    WHEN 'abandoned' THEN @status_lost_to_followup                    
                    WHEN 'lost' THEN @status_lost_to_followup 
                    WHEN 'treatment_refused' THEN @status_refused_treatment 
                    WHEN 'treatment_stopped_other' THEN @status_stopped_others 
                    WHEN 'treatment_stopped_side_effects' THEN @status_stopped_side_effects
                    WHEN 'refused_return_to_treatment' THEN @status_refused_treatment
                    WHEN 'traced_not_found' THEN @status_lost_to_followup                                                            
                    ELSE null 
                    END;                    
        ''')

        // In pseudocode, does something like:
        //   outcome_date = latest(treatment_status_date, regimen_outcome_date)
        //   if outcome_date is null:
        //       outcome_date = max_visit_date + 6 months
        //   if outcome_date is in the future:
        //       outcome_date = today
        executeMysql("Add outcome date if outcome present", '''
            # Top choice is the current treatment_status_date
            UPDATE hivmigration_programs_raw
                SET outcome_date = treatment_status_date;
                
            # Unless there's a regimen_outcome_date which is later than the treatment_status_date
            UPDATE hivmigration_programs_raw
                SET outcome_date = regimen_outcome_date WHERE outcome_date IS NULL OR regimen_outcome_date > outcome_date;
            
            # If no outcome date is readily available, use last visit date + 6 months as a proxy
            UPDATE hivmigration_programs_raw
                SET outcome_date = DATE(DATE_ADD(max_visit_date, INTERVAL 6 MONTH)) WHERE outcome_date IS NULL;            
            
            # If this results in a future outcome date, then just use the current date
            UPDATE hivmigration_programs_raw
                SET outcome_date = CURDATE() WHERE outcome_date > CURDATE();
                
            # Now, make sure we aren't setting an outcome date if the patient does not have an outcome
            UPDATE hivmigration_programs_raw
                SET outcome_date = null WHERE outcome is null;
        ''')

        executeMysql("Note where art_start_date > outcome_date", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type, warning_details, flag_for_review)
            SELECT
                p.person_id,
                'art_start_date', 
                hpr.art_start_date, 
                'Patient has ART start date(s) after patient outcome date',
                CONCAT('Outcome date: ', hpr.outcome_date),
                TRUE
            FROM hivmigration_programs_raw hpr
            JOIN hivmigration_patients p ON p.source_patient_id = hpr.source_patient_id
            WHERE hpr.art_start_date > hpr.outcome_date
            GROUP BY p.person_id;
        ''')

        executeMysql("Remove bad health center entries", '''
            UPDATE hivmigration_programs_raw
            SET health_center = IF (health_center = 0 OR health_center = 42, NULL, health_center),
                starting_health_center = IF (starting_health_center = 0 OR starting_health_center = 42, NULL, starting_health_center)
            ;
        ''')

        executeMysql("Ensure health_center is set and drop superfluous values for starting_health_center", '''
            UPDATE hivmigration_programs_raw
            SET
                health_center = COALESCE(health_center, starting_health_center, 1),
                starting_health_center = IF (
                        starting_health_center = health_center OR
                        health_center_transfer_date IS NULL OR
                        health_center_transfer_date > outcome_date OR  # TODO: investigate whether
                        health_center_transfer_date < enrollment_date, #   these conditions make sense
                    NULL,
                    starting_health_center)
            ;
        ''')

        // The following three blocks are where the enrollments get split.
        // For patients with no starting_health_center, we only create a single enrollment.
        // If a starting_health_center is present, we create two enrollments, "initial" and "current."

        executeMysql("Load single enrollments to hivmigration_programs table", '''
            INSERT INTO hivmigration_programs(
                source_patient_id, 
                location_id, 
                enrollment_date, 
                art_start_date, 
                outcome_date, 
                outcome, 
                treatment_status)
            SELECT 
                source_patient_id, 
                health_center, 
                enrollment_date, 
                art_start_date, 
                outcome_date, 
                outcome,
                openmrs_treatment_status  
            FROM hivmigration_programs_raw
            WHERE starting_health_center IS NULL;
        ''')

        executeMysql("Load initial enrollments into hivmigration_programs table", '''
            SET @outcome_transferred_out = (select concept_id from concept where uuid = '3cdd5c02-26fe-102b-80cb-0017a47871b2');

            insert into hivmigration_programs(
                source_patient_id, 
                location_id, 
                enrollment_date, 
                art_start_date, 
                outcome_date, 
                outcome)
            SELECT
                source_patient_id,
                starting_health_center,
                enrollment_date,
                IF (art_start_date < health_center_transfer_date, art_start_date, NULL),
                health_center_transfer_date,
                @outcome_transferred_out
            FROM hivmigration_programs_raw
            WHERE starting_health_center IS NOT NULL
            ;
        ''')

        executeMysql("Load current enrollments into hivmigration_programs table", '''
            insert into hivmigration_programs(
                source_patient_id, 
                location_id, 
                enrollment_date, 
                art_start_date, 
                outcome_date, 
                outcome,
                treatment_status)
            SELECT
                source_patient_id,
                health_center,
                health_center_transfer_date,
                art_start_date,  # this will be before the transfer_date in cases where ART started at the first health center
                outcome_date,
                outcome,
                openmrs_treatment_status
            FROM hivmigration_programs_raw
            WHERE starting_health_center IS NOT NULL
            ;
        ''')

        executeMysql("Log warning if completion date is before enrollment date", '''
            INSERT INTO hivmigration_data_warnings (
                openmrs_patient_id, 
                field_name, 
                field_value, 
                warning_type, 
                warning_details, 
                flag_for_review)
            SELECT
                p.person_id as openmrs_patient_id,
                'enrollment_date' as field_name, 
                h.enrollment_date as field_value, 
                'Patient has program enrollment date after program completion date' as warning_type,                
                CASE 
                    WHEN  p.patient_created_date < h.outcome_date  THEN 'Setting enrollment date to patient_created_date' 
                    ELSE 'Setting enrollment date to  completion date' 
                    END as warning_details,
                TRUE as flag_for_review
            FROM hivmigration_programs h
            JOIN hivmigration_patients p ON p.source_patient_id = h.source_patient_id
            WHERE h.outcome_date < h.enrollment_date
            GROUP BY p.person_id;
        ''')

        executeMysql("Adjust patient program enrollment date if it is after the program completion date", '''
            UPDATE  hivmigration_programs h,  
                ( SELECT outcome, outcome_date, patient_created_date, person_id, source_patient_id  
                from hivmigration_patients ) p
            SET h.enrollment_date = CASE 
            WHEN  p.patient_created_date < h.outcome_date  THEN p.patient_created_date 
                    ELSE h.outcome_date 
                    END
            WHERE h.source_patient_id = p.source_patient_id 
                and h.outcome_date < h.enrollment_date;
        ''')
        
        executeMysql("Load to patient_program table", '''
            SET @hiv_program = (SELECT program_id FROM program WHERE uuid = "b1cb1fc1-5190-4f7a-af08-48870975dafc");
            
            insert into patient_program(
                patient_program_id, 
                patient_id, 
                program_id, 
                date_enrolled, 
                date_completed, 
                location_id, 
                outcome_concept_id, 
                creator, 
                date_created, 
                uuid)
            select
                h.patient_program_id, 
                p.person_id,
                @hiv_program,
                h.enrollment_date, 
                h.outcome_date, 
                hc.openmrs_id,
                h.outcome,
                1,
                date_format(curdate(), '%Y-%m-%d %T'),
                uuid()
            from 
                hivmigration_programs h 
            inner join 
                hivmigration_patients p on h.source_patient_id = p.source_patient_id 
            left outer join
                hivmigration_health_center hc on h.location_id = hc.hiv_emr_id;
                
                        
            -- Add Treatment statuses as records to the PATIENT_STATE table
            
            SET @status_lost_to_followup = concept_from_mapping('PIH', 'LOST TO FOLLOWUP');
            SET @status_refused_treatment = concept_from_mapping('PIH', 'TREATMENT NEVER STARTED - PATIENT REFUSED');
            SET @status_stopped_side_effects = concept_from_mapping('PIH', 'TREATMENT STOPPED - SIDE EFFECTS');
            SET @status_stopped_others = concept_from_mapping('PIH', 'Other treatment stopped');   
            
            set @hiv_workflow_id = (SELECT program_workflow_id FROM program_workflow WHERE uuid='aba55bfe-9490-4362-9841-0c476e379889'); -- HIV_TREATMENT_STATUS
            set @ltfu_state = (select program_workflow_state_id from program_workflow_state where program_workflow_id = @hiv_workflow_id and concept_id = @status_lost_to_followup);
            set @refused_treatment_state = (select program_workflow_state_id from program_workflow_state where program_workflow_id = @hiv_workflow_id and concept_id = @status_refused_treatment);
            set @treatment_stopped_side_effects_state = (select program_workflow_state_id from program_workflow_state where program_workflow_id = @hiv_workflow_id and concept_id = @status_stopped_side_effects);
            set @treatment_stopped_other_state = (select program_workflow_state_id from program_workflow_state where program_workflow_id = @hiv_workflow_id and concept_id = @status_stopped_others);
                 
            insert into patient_state(
                patient_program_id,
                state,
                start_date,
                end_date,
                creator,
                date_created,
                voided,
                uuid)
            select 
                h.patient_program_id,
                case h.treatment_status
                    when @status_lost_to_followup then @ltfu_state 
                    when @status_refused_treatment then @refused_treatment_state 
                    when @status_stopped_side_effects then @treatment_stopped_side_effects_state 
                    when @status_stopped_others then @treatment_stopped_other_state
                end as state,
                h.enrollment_date,
                h.outcome_date,  
                1,
                date_format(curdate(), '%Y-%m-%d %T'),
                0,
                uuid()
            from 
                hivmigration_programs h 
            where h.treatment_status is not null;    
        ''')

    }

    void revert() {
        executeMysql("drop table if exists hivmigration_programs_raw;")
        executeMysql("drop table if exists hivmigration_programs;")
        executeMysql("drop table if exists hivmigration_outcome;")
        clearTable("patient_state")
        clearTable("patient_program")
    }
}
