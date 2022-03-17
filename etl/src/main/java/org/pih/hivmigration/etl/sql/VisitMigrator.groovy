package org.pih.hivmigration.etl.sql

class VisitMigrator extends SqlMigrator {

    void migrate() {

        executeMysql("Create visits from existing encounters and add them to the encounters", '''
            -- Get visit type "Clinic or Hospital Visit"
            SET @visit_type_id = (SELECT visit_type_id FROM visit_type WHERE uuid = 'f01c54cb-2225-471a-9cd5-d348552c337c');
            SET @registration_et = (SELECT encounter_type_id FROM encounter_type WHERE name = 'Enregistrement de patient');             
            SET @drug_order_et = (SELECT encounter_type_id FROM encounter_type WHERE uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002'); 
            set @comment_et = (SELECT encounter_type_id FROM encounter_type WHERE uuid = 'c30d6e06-0f00-460a-8f81-3c39a1853b56');
            
            INSERT INTO visit
            (patient_id, visit_type_id, date_started, date_stopped, creator, date_created, voided, uuid)
            SELECT  e.patient_id, @visit_type_id, e.date_started, e.date_stopped, e.creator, now(), 0, uuid()
            FROM
                (
                    SELECT patient_id,
                           Min(encounter_datetime) AS date_started,  -- encounter_datetime is always midnight
                           Addtime(Max(encounter_datetime), '23:59:59') AS date_stopped,
                           creator
                    FROM   encounter
                    WHERE  encounter_type not in (@registration_et, @drug_order_et, @comment_et) AND visit_id IS NULL
                    GROUP  BY patient_id, Date(encounter_datetime)
                              
                ) AS e;
        ''')

        executeMysql("Associate encounters with visit on the same date if otherwise is not associated with a visit", '''
            SET @registration_et = (SELECT encounter_type_id FROM encounter_type WHERE name = 'Enregistrement de patient');             
            SET @drug_order_et = (SELECT encounter_type_id FROM encounter_type WHERE uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002'); 
            set @comment_et = (SELECT encounter_type_id FROM encounter_type WHERE uuid = 'c30d6e06-0f00-460a-8f81-3c39a1853b56');
            UPDATE encounter e
                INNER JOIN visit v
                ON e.patient_id = v.patient_id
                    AND Date(e.encounter_datetime) = Date(v.date_started)
            SET    e.visit_id = v.visit_id
            WHERE encounter_type  not in (@registration_et, @drug_order_et)
              AND e.visit_id IS NULL;
        ''')

        executeMysql("Set visit location to the most frequently occurring encounter location in the visit, excluding unknown location", '''
            SET @unknown_location_id = 1;
            UPDATE visit v
            SET    v.location_id = 
                   ( select e.location_id from encounter e where e.visit_id = v.visit_id and e.location_id != @unknown_location_id group by e.location_id order by count(*) desc, e.location_id desc limit 1)
            WHERE  v.location_id is null
            ;
        ''')

        executeMysql("Set visit location to Unknown Location if null", '''
            SET @unknown_location_id = 1;
            UPDATE visit v set v.location_id = @unknown_location_id where v.location_id is null;
        ''')

        executeMysql("If a encounter location is Unknown and it's Visit Location is *not* Unknown, update encounter with that location", ''' 
            SET @unknown_location_id = 1;
            UPDATE encounter e
                INNER JOIN visit v
                ON e.visit_id = v.visit_id
            SET e.location_id = v.location_id
            WHERE e.location_id = @unknown_location_id and v.location_id != @unknown_location_id
        ''')

        executeMysql("Log visits that start prior to patient birthdate",
                '''
                    INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type, warning_details, flag_for_review)
                    select   p.person_id, 'date_started', v.date_started, 'Visit start date prior to birthdate',
                             concat('Birthdate: ', p.birthdate, ', estimated: ', p.birthdate_estimated), TRUE
                    from     visit v
                    inner join person p on v.patient_id = p.person_id
                    where
                           (p.birthdate_estimated = 0 and p.birthdate > v.date_started)
                    or     (p.birthdate_estimated = 1 and date_add(p.birthdate, INTERVAL least(-1, ceil(timestampdiff(YEAR, p.birthdate, ifnull(p.death_date, now())) * -0.5)) YEAR) > v.date_started)
                    ;
            ''')

    }

    void revert() {
        executeMysql("Clear visit IDs from encounters", '''
            UPDATE encounter
            SET visit_id = NULL;
        ''')
        clearTable("visit")
    }
}
