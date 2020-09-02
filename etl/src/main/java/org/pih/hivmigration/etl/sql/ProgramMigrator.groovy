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
                outcome varchar(255)
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
                outcome varchar(255)
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
            SET @outcome_died = (select concept_id from concept where uuid = '3cdd446a-26fe-102b-80cb-0017a47871b2');
            SET @outcome_lost_to_followup = (select concept_id from concept where uuid = '3ceb0ed8-26fe-102b-80cb-0017a47871b2');
            SET @outcome_transferred_out = (select concept_id from concept where uuid = '3cdd5c02-26fe-102b-80cb-0017a47871b2');
            SET @outcome_treatment_stopped = (select concept_id from concept where uuid = '3cdc0d7a-26fe-102b-80cb-0017a47871b2');

            UPDATE hivmigration_programs_raw
            SET
                # Enrollment Date in program is earlier of minimum encounter and art start date
                enrollment_date = LEAST (IFNULL(min_visit_date, art_start_date), IFNULL(art_start_date, min_visit_date)),                
                outcome = CASE treatment_status
                    WHEN 'died' THEN @outcome_died
                    WHEN 'lost' THEN @outcome_lost_to_followup
                    WHEN 'abandoned' THEN @outcome_lost_to_followup
                    WHEN 'transferred_out' THEN @outcome_transferred_out
                    WHEN 'treatment_refused' THEN @outcome_treatment_stopped  # TODO: this might be its own outcome
                    WHEN 'treatment_stopped' THEN @outcome_treatment_stopped
                    WHEN 'treatment_stopped_side_effects' THEN @outcome_treatment_stopped
                    WHEN 'treatment_stopped_other' THEN @outcome_treatment_stopped
                    END
            ;
        ''')

        // In pseudocode, does something like:
        //   outcome_date = latest(treatment_status_date, regimen_oucome_date, enrollment_date)
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
            # And if the enrollment date (which is the earlier of minimum encounter and art start date) is later
            # than either of those, use that
            UPDATE hivmigration_programs_raw
                SET outcome_date = enrollment_date WHERE outcome_date IS NULL OR enrollment_date > outcome_date;
            UPDATE hivmigration_programs_raw
                SET outcome_date = DATE(DATE_ADD(max_visit_date, INTERVAL 6 MONTH)) WHERE outcome IS NULL;            
            UPDATE hivmigration_programs_raw
                SET outcome_date = CURDATE() WHERE outcome_date > CURDATE();
        ''')

        // TODO: investigate cases where art_start_date > outcome_date

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
            INSERT INTO hivmigration_programs
                (source_patient_id, location_id, enrollment_date, art_start_date, outcome_date, outcome)
            SELECT source_patient_id, health_center, enrollment_date, art_start_date, outcome_date, outcome FROM hivmigration_programs_raw
            WHERE starting_health_center IS NULL
            ;
        ''')

        executeMysql("Load initial enrollments into hivmigration_programs table", '''
            SET @outcome_transferred_out = (select concept_id from concept where uuid = '3cdd5c02-26fe-102b-80cb-0017a47871b2');

            insert into hivmigration_programs
                (source_patient_id, location_id, enrollment_date, art_start_date, outcome_date, outcome)
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
            insert into hivmigration_programs
                (source_patient_id, location_id, enrollment_date, art_start_date, outcome_date, outcome)
            SELECT
                source_patient_id,
                health_center,
                health_center_transfer_date,
                art_start_date,  # this will be before the transfer_date in cases where ART started at the first health center
                outcome_date,
                outcome
            FROM hivmigration_programs_raw
            WHERE starting_health_center IS NOT NULL
            ;
        ''')

        executeMysql("Load to patient_program table", '''
            SET @hiv_program = (SELECT program_id FROM program WHERE uuid = "b1cb1fc1-5190-4f7a-af08-48870975dafc");
            
            insert into patient_program
                (patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, uuid)
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
                hivmigration_health_center hc on h.location_id = hc.hiv_emr_id
            ; 
        ''')
        
        // TODO: figure out how patient state should work
    }

    void revert() {
        executeMysql("drop table if exists hivmigration_programs_raw;")
        executeMysql("drop table if exists hivmigration_programs;")
        executeMysql("drop table if exists hivmigration_outcome;")
        clearTable("patient_state")
        clearTable("patient_program")
    }
}
