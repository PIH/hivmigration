package org.pih.hivmigration.etl.sql

class ProgramMigrator extends SqlMigrator {

    void migrate() {
        executeMysql("Create staging table to receive program data", '''
            create table hivmigration_programs_raw (
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
                enrollment_date date,
                outcome_date date,
                outcome varchar(255)
            );
        ''')

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

        executeMysql("Create outcome lookup table", '''
            create table hivmigration_outcome (
              id int PRIMARY KEY AUTO_INCREMENT,
            	outcome varchar(255),
            	outcome_concept_id int
            );
            
            SET @outcome_died = (select concept_id from concept where uuid = '3cdd446a-26fe-102b-80cb-0017a47871b2');
            SET @outcome_lost_to_followup = (select concept_id from concept where uuid = '3ceb0ed8-26fe-102b-80cb-0017a47871b2');
            SET @outcome_transferred_out = (select concept_id from concept where uuid = '3cdd5c02-26fe-102b-80cb-0017a47871b2');
            SET @outcome_treatment_stopped = (select concept_id from concept where uuid = '3cdc0d7a-26fe-102b-80cb-0017a47871b2');
            
            insert into hivmigration_outcome(outcome, outcome_concept_id) values("DIED", @outcome_died);
            insert into hivmigration_outcome(outcome, outcome_concept_id) values("LOST", @outcome_lost_to_followup);
            insert into hivmigration_outcome(outcome, outcome_concept_id) values("TREATMENT_STOPPED", @outcome_treatment_stopped);
            insert into hivmigration_outcome(outcome, outcome_concept_id) values("TRANSFERRED_OUT", @outcome_transferred_out);
            insert into hivmigration_outcome(outcome, outcome_concept_id) values("TRANSFERRED_INTERNALLY", @outcome_transferred_out);
        ''')

        setAutoIncrement("patient_program", "(select ifnull(max(patient_program_id)+1, 1) from patient_program)")
        
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
                d.TREATMENT_STATUS != 'test' 
            and 
                d.patient_id = e.patient_id
            and
                d.patient_id = r.patient_id(+)
            and
                d.patient_id = o.patient_id(+)
        ''')

        executeMysql("Add enrollment_date and outcome", '''
            UPDATE hivmigration_programs_raw
            SET
                # Enrollment Date in program is earlier of minimum encounter and art start date
                enrollment_date = LEAST (IFNULL(min_visit_date, art_start_date), IFNULL(art_start_date, min_visit_date)),                
                outcome = CASE treatment_status
                    WHEN 'died' THEN 'DIED'
                    WHEN 'lost' THEN 'LOST' 
                    WHEN 'abandoned' THEN 'LOST'
                    WHEN 'transferred_out' THEN 'TRANSFERRED_OUT'
                    WHEN 'treatment_refused' THEN 'TREATMENT_STOPPED'  # TODO: this might be its own outcome
                    WHEN 'treatment_stopped' THEN 'TREATMENT_STOPPED'
                    WHEN 'treatment_stopped_side_effects' THEN 'TREATMENT_STOPPED'
                    WHEN 'treatment_stopped_other' THEN 'TREATMENT_STOPPED'
                    END
            ;
        ''')


        executeMysql("Add outcome date if outcome present", '''
            UPDATE hivmigration_programs_raw
            SET
                # Outcome date is the most recent of treatment_status_date, regimen_outcome_date, and enrollment date
                outcome_date = IFNULL(
                    GREATEST (
                        COALESCE(treatment_status_date, regimen_outcome_date, '1800-01-01'),
                        IFNULL(regimen_outcome_date, '1800-01-01'),
                        enrollment_date
                    ),
                    DATE_ADD(max_visit_date, INTERVAL 6 MONTH)
                )
            WHERE OUTCOME IS NOT NULL
            ;
        ''')

        executeMysql("Fix ART start date in the 4-5 cases where it starts after the outcome", '''
            UPDATE hivmigration_programs_raw
            SET art_start_date = LEAST (art_start_date, outcome_date)
            WHERE art_start_date IS NOT NULL AND outcome_date IS NOT NULL
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
                        health_center_transfer_date > outcome_date OR
                        health_center_transfer_date < enrollment_date,
                    NULL,
                    starting_health_center)
            ;
        ''')

        executeMysql("Load single enrollments to hivmigration_programs table", '''
            INSERT INTO hivmigration_programs
                (source_patient_id, location_id, enrollment_date, art_start_date, outcome_date, outcome)
            SELECT source_patient_id, health_center, enrollment_date, art_start_date, outcome_date, outcome FROM hivmigration_programs_raw
            WHERE starting_health_center IS NULL
            ;
        ''')

        executeMysql("Load initial enrollments into hivmigration_programs table", '''
            insert into hivmigration_programs
                (source_patient_id, location_id, enrollment_date, art_start_date, outcome_date, outcome)
            SELECT
                source_patient_id,
                starting_health_center,
                enrollment_date,
                IF (art_start_date < health_center_transfer_date, art_start_date, NULL),
                health_center_transfer_date,
                'TRANSFERRED_INTERNALLY'
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
                IF (art_start_date < health_center_transfer_date, health_center_transfer_date, art_start_date),
                outcome_date,
                outcome
            FROM hivmigration_programs_raw
            WHERE starting_health_center IS NOT NULL
            ;
        ''')

        executeMysql("Load to patient_program table", '''
            insert into patient_program
                (patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, uuid)
            select
                h.patient_program_id, 
                p.person_id,
                2,
                h.enrollment_date, 
                h.outcome_date, 
                hc.openmrs_id,
                o.outcome_concept_id, 
                1,
                date_format(curdate(), '%Y-%m-%d %T'),
                uuid()
            from 
                hivmigration_programs h 
            inner join 
                hivmigration_patients p on h.source_patient_id = p.source_patient_id 
            left outer join
                hivmigration_health_center hc on h.location_id = hc.hiv_emr_id
            left outer join 
                hivmigration_outcome o on h.outcome = o.outcome
            ; 
        ''')
        
        executeMysql("Add patient state", '''
            insert into patient_state
                (patient_program_id, state, start_date, end_date, creator, date_created, uuid)
            select
                patient_program_id,
                if (art_start_date is null or art_start_date > enrollment_date, 2, 1),
                if (art_start_date is null, enrollment_date, art_start_date), 
                if (art_start_date is null, outcome_date, art_start_date),
                1,
                date_format(curdate(), '%Y-%m-%d %T'),
                uuid()
            from hivmigration_programs
            ;
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
