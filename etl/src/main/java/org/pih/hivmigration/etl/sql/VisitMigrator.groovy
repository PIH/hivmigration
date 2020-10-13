package org.pih.hivmigration.etl.sql

class VisitMigrator extends SqlMigrator {

    void migrate() {

        executeMysql("Create visits from existing encounters", '''
            -- Get visit type "Clinic or Hospital Visit"
            SET @visit_type_id = (SELECT visit_type_id FROM visit_type WHERE uuid = 'f01c54cb-2225-471a-9cd5-d348552c337c');
            SET @encounter_type_exclusions = '';  -- a comma-separated string like '1,2,3'
            
            INSERT INTO visit
            (patient_id,   visit_type_id,  date_started,   date_stopped,   location_id,   creator,   date_created, voided, uuid)
            SELECT  e.patient_id, @visit_type_id, e.date_started, e.date_stopped, e.location_id, e.creator, now(),     0, uuid()
            FROM
                (
                    SELECT patient_id,
                           Min(encounter_datetime) AS date_started,  -- encounter_datetime is always midnight
                           Addtime(Max(encounter_datetime), '23:59:59') AS date_stopped,
                           Max(location_id) as location_id,  /* Avoid using 'Unknown Location' if there is another location available */
                           creator
                    FROM   encounter
                    WHERE  FIND_IN_SET(encounter_type, @encounter_type_exclusions) = 0
                    GROUP  BY patient_id,
                              Date(encounter_datetime)
                              -- TODO: Group by location as well? see: https://pihemr.atlassian.net/browse/UHM-4834
                ) AS e;
            
            -- Add visit IDs to their encounters
            UPDATE encounter e
                INNER JOIN visit v
                ON e.patient_id = v.patient_id
                    AND Date(e.encounter_datetime) = Date(v.date_started)
            SET    e.visit_id = v.visit_id
            WHERE FIND_IN_SET(e.encounter_type, @encounter_type_exclusions) = 0;
        ''')

        executeMysql("Log visits with encounters at multiple locations",
                 '''
                INSERT INTO hivmigration_data_warnings (patient_id, encounter_id, encounter_date, field_name, field_value, note)  
                select
                    src_p.source_patient_id as patient_id,
                    src_e.source_encounter_id as encounter_id,
                    src_e.encounter_date as encounter_date,
                    'encounter_location' as field_name,
                    CONCAT(e_loc.name, ", ", v_loc.name) as field_value,
                    'Patient has encounters at different locations on same day' as note
                from encounter e, visit v, location v_loc, location e_loc, hivmigration_patients src_p, hivmigration_encounters src_e
                where e.visit_id=v.visit_id
                    and e.location_id != v.location_id
                    and src_p.person_id = e.patient_id
                    and src_e.encounter_id = e.encounter_id
                    and e.location_id = e_loc.location_id
                    and v.location_id = v_loc.location_id;
            ''')

    }

    void revert() {
        clearTable("visit")
    }
}
