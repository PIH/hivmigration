package org.pih.hivmigration.etl.sql

class UnknownEncounterLocationsMigrator extends SqlMigrator {

    @Override
    void migrate() {

        executeMysql("Update all encounters with unknown location with program location when matching enrollment found on encounter date",
        '''
            set @unknown_location = (select location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f');
            set @hiv_program = (select program_id from program where uuid='b1cb1fc1-5190-4f7a-af08-48870975dafc');
    
            # create temp table mapping encounter to new location
            drop table if exists tmp_current_program_location_to_encounter;
            create temporary table tmp_current_program_location_to_encounter (patient_id int, encounter_id int, encounter_datetime datetime, location_id int);
    
            # populate temp table by finding all encounters with unknown location, and joining on any patient program active on that encounter date
            insert into tmp_current_program_location_to_encounter
                select 
                    e.patient_id, e.encounter_id, e.encounter_datetime, pp.location_id
                from 
                    encounter e, patient_program pp
                where
                    e.patient_id = pp.patient_id and e.encounter_datetime >= pp.date_enrolled and (pp.date_completed is null || e.encounter_datetime <= pp.date_completed)
                    and e.location_id= @unknown_location
                    and pp.program_id = @hiv_program;
    
            # now update these encounters
            update encounter e, tmp_current_program_location_to_encounter tmp
                set e.location_id = tmp.location_id
                where e.encounter_id = tmp.encounter_id;
    
            # insert a data warning noting this change
            insert into hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, warning_details, encounter_date, flag_for_review, warning_type)
                select 
                    patient_id as openmrs_patient_id, encounter_id as openmrs_encounter_id,  CONCAT('Updated to location :', location_id) as warning_details, 
                    DATE(encounter_datetime) as encounter_date, 0 as flag_for_review, 'Encounter location changed from Unknown to location of Current Program Enrollment' as warning_type
                from 
                    tmp_current_program_location_to_encounter;
        ''');

        executeMysql("Update any remaining encounters with unknown location with program location when previous enrollment found prior to encounter date",
        '''
            set @unknown_location = (select location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f');
            set @hiv_program = (select program_id from program where uuid='b1cb1fc1-5190-4f7a-af08-48870975dafc');
            
            # create temporary table mapping most recent previous program date to encounter
            drop table if exists tmp_most_recent_previous_program_date_to_encounter;
            create temporary table tmp_most_recent_previous_program_date_to_encounter (encounter_id int, date_completed datetime);
    
            # populate temp table by taking all encounters with unknown location, and joining on any patient program that were completed before the encounter date
            # in the event of multiple previous programs per encounter, group by encounter and take the max date (ie, completion date closest to encounter date)
            insert into tmp_most_recent_previous_program_date_to_encounter
            select 
                e.encounter_id, max(pp.date_completed) 
            from 
                encounter e, patient_program pp
            where
                e.patient_id = pp.patient_id and e.encounter_datetime > pp.date_completed
                and e.location_id = @unknown_location
                and pp.program_id = @hiv_program 
            group by 
                e.encounter_id;
    
            # create temp table to map most recent program location to encounter
            create temporary table tmp_most_recent_previous_program_location_to_encounter (patient_id int, encounter_id int, encounter_datetime datetime, location_id int);
    
            # populate most recent program location tmp table by joining encounter and patient_program using most_recent_previous_program_date_to_encounter temp table
            insert into tmp_most_recent_previous_program_location_to_encounter
                select 
                    e.patient_id, e.encounter_id, e.encounter_datetime, pp.location_id
                from 
                    tmp_most_recent_previous_program_date_to_encounter tmp, patient_program pp, encounter e
                where 
                    e.encounter_id = tmp.encounter_id and pp.date_completed = tmp.date_completed and pp.patient_id = e.patient_id;
    
            # do the actual update
            update encounter e, tmp_most_recent_previous_program_location_to_encounter tmp
                set e.location_id = tmp.location_id
                where e.encounter_id = tmp.encounter_id;
    
            # insert a data warning noting this change
            insert into hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, warning_details, encounter_date, flag_for_review, warning_type)
                select 
                    patient_id as openmrs_patient_id, encounter_id as openmrs_encounter_id,  CONCAT('Updated to location :', location_id) as warning_details, 
                    DATE(encounter_datetime) as encounter_date, 0 as flag_for_review, 
                    'Encounter location changed from Unknown to location of Most Recent Previous Program Enrollment' as warning_type
                from 
                    tmp_most_recent_previous_program_location_to_encounter;
        ''');

        executeMysql("Update any remaining encounters with unknown location with program location when subsequent enrollment found after encounter date",
        '''
            set @unknown_location = (select location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f');
            set @hiv_program = (select program_id from program where uuid='b1cb1fc1-5190-4f7a-af08-48870975dafc');
    
            # create temporary table mapping most recent subsequent program date to encounter
            drop table if exists tmp_most_recent_subsequent_program_date_to_encounter;
            create temporary table tmp_most_recent_subsequent_program_date_to_encounter (encounter_id int, date_enrolled datetime);
    
            # populate temp table by taking all encounters with unknown location, and joining on any patient program that were started after the encounter date
            # in the event of multiple previous programs per encounter, group by encounter and take the min date (ie, enrollment date closest to encounter date)
            insert into tmp_most_recent_subsequent_program_date_to_encounter
            select 
                e.encounter_id, min(pp.date_enrolled) from encounter e, patient_program pp 
            where
                e.patient_id = pp.patient_id and e.encounter_datetime < pp.date_enrolled
                and e.location_id = @unknown_location
                and pp.program_id = @hiv_program
            group by 
                e.encounter_id;
    
            # create temp table to map most recent program location to encounter
            drop temporary table if exists  tmp_most_recent_subsequent_program_location_to_encounter;
            create temporary table tmp_most_recent_subsequent_program_location_to_encounter (patient_id int, encounter_id int, encounter_datetime datetime, location_id int);
    
            # populate most recent program location tmp table by joining encounter and patient_program using most_recent_subsequent_program_date_to_encounter temp table
            insert into tmp_most_recent_subsequent_program_location_to_encounter
                select 
                    e.patient_id, e.encounter_id, e.encounter_datetime, pp.location_id
                from 
                    tmp_most_recent_subsequent_program_date_to_encounter tmp, patient_program pp, encounter e
                where 
                    e.encounter_id = tmp.encounter_id and pp.date_enrolled = tmp.date_enrolled and pp.patient_id = e.patient_id;
        
            # do the actual update
            update encounter e, tmp_most_recent_subsequent_program_location_to_encounter tmp
                set e.location_id = tmp.location_id
                where e.encounter_id = tmp.encounter_id;
    
            # log a data warning
            insert into hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, warning_details, encounter_date, flag_for_review, warning_type)
                select 
                    patient_id as openmrs_patient_id, encounter_id as openmrs_encounter_id,  CONCAT('Updated to location :', location_id) as warning_details, 
                    DATE(encounter_datetime) as encounter_date, 0 as flag_for_review, 
                    'Encounter location changed from Unknown to location of Most Recent Subsequent Program Enrollment' as warning_type
               from tmp_most_recent_subsequent_program_location_to_encounter;
        ''');

        executeMysql("Update any remaining encounters with unknown location with overall patient location, if never enrolled", '''
            set @unknown_location = (select location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f');
        
            # create temporary table of encounters affected
            drop table if exists tmp_overall_location_to_encounter;
            create temporary table tmp_overall_location_to_encounter (encounter_id int, location_id int);
        
            insert into tmp_overall_location_to_encounter (encounter_id, location_id)
            select      e.encounter_id, hc.openmrs_id
            from        encounter e 
            inner join  hivmigration_patients p on e.patient_id = p.person_id
            inner join  hivmigration_health_center hc on p.health_center = hc.hiv_emr_id
            where       e.location_id = @unknown_location
            ;
            
            update encounter e, tmp_overall_location_to_encounter tmp
            set e.location_id = tmp.location_id
            where e.encounter_id = tmp.encounter_id
            ;
    
            # log a data warning
            insert into hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, warning_details, encounter_date, flag_for_review, warning_type)
                select      e.patient_id, e.encounter_id,  CONCAT('Updated to location :', e.location_id) as warning_details,
                            DATE(encounter_datetime) as encounter_date, 0 as flag_for_review, 
                            'Encounter location changed from Unknown to overall Health Center' as warning_type
                from        encounter e
                inner join  tmp_overall_location_to_encounter t on e.encounter_id = t.encounter_id
            ;
        ''');

        executeMysql("Update any unknown visit locations that we now can resolve with the additional encounter information",
'''
            set @unknown_location = (select location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f');
            
            # create temporary table to store new visit location
            drop table if exists tmp_new_visit_location;
            create temporary table tmp_new_visit_location  (visit_id int, location_id int);
    
            # find all visits that have a unknown location but also have or more encounters with a location other than Unknown Location
            # pull the location off the matching visits (arbitrarily picking the "max" if multiple possibilities found)
            insert into tmp_new_visit_location
            select 
                v.visit_id, max(e.location_id)
            from 
                visit v, encounter e
            where 
                e.location_id != @unknown_location and v.location_id = @unknown_location and e.visit_id = v.visit_id 
            group by 
                e.visit_id;
    
            # update the visit locations
            update tmp_new_visit_location tmp, visit v
                set v.location_id = tmp.location_id
                where tmp.visit_id = v.visit_id;

        ''');

        executeMysql("Flag any encounters that are still unknown",
        '''

            set @unknown_location = (select location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f');
    
            insert into hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, flag_for_review, warning_type)
                select 
                    patient_id as openmrs_patient_id, encounter_id as openmrs_encounter_id, DATE(encounter_datetime) as encounter_date,
                    1 as flag_for_review, 'Unable to determine encounter location; set as unknown' as warning_type
               from 
                encounter 
                   where location_id=@unknown_location;
        ''')

        executeMysql("Log visits with encounters at multiple locations",
                '''

               set @unknown_location = (select location_id from location where name='Unknown Location');
                
               INSERT INTO hivmigration_data_warnings (openmrs_patient_id, openmrs_encounter_id, encounter_date, field_name, field_value, warning_type, flag_for_review)  
               select
                    e.patient_id,
                    e.encounter_id,
                    DATE(e.encounter_datetime) as encounter_date,
                    'encounter_location' as field_name,
                    CONCAT(e_loc.name, ", ", v_loc.name) as field_value,
                    'Patient has encounters at different locations on same day' as warning_type,
                    TRUE as flag_for_review
                from encounter e, visit v, location v_loc, location e_loc
                    where e.visit_id=v.visit_id
                    and e.location_id != v.location_id 
                    and e.location_id != @unknown_location 
                    and v.location_id != @unknown_location 
                    and e.location_id = e_loc.location_id
                    and v.location_id = v_loc.location_id;
            ''')

    }

    @Override
    void revert() {
        // not implemented
    }
}
