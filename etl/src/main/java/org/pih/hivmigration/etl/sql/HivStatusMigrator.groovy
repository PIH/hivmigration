package org.pih.hivmigration.etl.sql

class HivStatusMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create staging table for HIV_STATUS", '''
            create table hivmigration_hiv_status (
            
              -- Source data
              source_status_id int,
              source_encounter_id int,
              source_patient_id int,
              source_status_date date,
              source_date_unknown boolean,
              source_date_entered date,
              source_entered_by int,
              source_current boolean,
              source_test_result char(1),
              source_test_location varchar(200),
              source_test_location_other varchar(200),
              
              -- Derived data
              derived_status_date date,
              derived_encounter_id int
            );
        ''')

        loadFromOracleToMySql('''
            insert into hivmigration_hiv_status (
              source_status_id int,
              source_encounter_id int,
              source_patient_id int,
              source_status_date date,
              source_date_unknown boolean,
              source_date_entered date,
              source_entered_by int,
              source_current boolean,
              source_test_result char(1),
              source_test_location varchar(200),
              source_test_location_other varchar(200)    
            )
            values(?,?,?,?,?,?,?,?,?,?,?) 
            ''', '''
            select 
                s.HIV_STATUS_ID
                s.ENCOUNTER_ID,
                s.PATIENT_ID,
                s.STATUS_DATE,
                decode(s.DATE_UNKNOWN_P, 't', 1, 0),
                s.ENTERED_DATE,
                s.ENTERED_BY,
                decode(s.TYPE, 'current', 1, 0),
                s.HIV_POSITIVE_P,
                s.TEST_LOCATION,
                s.TEST_LOCATION_OTHER
            from 
                HIV_HIV_STATUS s, HIV_DEMOGRAPHICS_REAL d  
            where 
                s.PATIENT_ID = d.PATIENT_ID;
        ''')

        // Try to determine what encounter a row is associated with

        // If source_encounter_id is null, then:
        //  - Look for matching intake/followup encounters whose encounter_datetime = s.status_date for the same patient
        //  - Look for matching intake/followup encounters where a row in hiv_encounters_aud.modified = s.entered_date
        //  - Evaluate from here.  Continue adding conditions until most/all rows have encounters
        


        // Any row where source_encounter_id is not null can be migrated into observations

        // source_status_date : CIEL:164400 (HIV test date) - on current form
        // source_test_result : CIEL:163722 (Rapid test for HIV) - not on current form
        //  -- ? = CIEL:1138 (Indeterminate)
        //  -- t = CIEL:703 (Positive)
        //  -- f = CIEL:664 (Negative)
        // source_test_location:  CIEL:159936 (Point of HIV testing)
        //  -- TODO Need to map in source answers.  Below is what is on the current v4 openmrs form.
        //  -- CIEL:159940 (vct)
        //  -- CIEL:159937 (MCH)
        //  -- CIEL:160542 (Outpatient)
        //  -- CIEL:159939 (Mobile)
        //  -- PIH:2714 (Family planning clinic)
        //  -- PIH:2235 (inpatient maternity)
        //  -- PIH:6298 (surgery)
        //  -- PIH:7891 (inpatient internal med)
        //  -- PIH:2717 (inpatient peds)
        //  source_test_location_other : CIEL:159936 (question), CIEL:5622 (other), value put in comments

        /*






-- hiv_hiv_status is only collected on the intake form when it is associated with an encounter
create or replace view hiv_first_hiv_positive_status as
select  patient_id, derived_status_date as status_date, entered_date, entered_by, test_location, test_location_other, date_unknown_p
from    (
            select  s.*, nvl(s.status_date, e.encounter_date) as derived_status_date,
                    row_number() over (partition by s.patient_id order by s.status_date asc) as sort_order
            from    hiv_hiv_status s
                        left join hiv_encounters e on s.encounter_id = e.encounter_id
            where   hiv_positive_p = 't'
              and     nvl(s.status_date, e.encounter_date) is not null
        )
where   sort_order = 1;


On the forms on which no encounter_id is associated with a status row:

If forms are entered out of order (back-entry, etc), the hiv_hiv_status table might be set to something
other than the most recently updated value

If we wish to migrate these as observations, it is not clear exactly which encounters to associate them with

Due to the use of the trigger to set existing values to previous:

It might be unclear whether “previous” entries represent valid observations from previous encounters or
corrected/voided observations from when encounters are updated.

Due to the RDV v2 form doing things inconsistently:

Setting the status_date to “today” when the form is entered, and not associating encounter_id with the row,
will mean that retrospective encounters will lead to invalid dates (some small, some large)


Modify the logic in the view above.  Instead of nvl(first_status_date, first_tr_positive_date),
change it to min(first_status_date, first_tr_positive_date).

Take the result of this value and record it as an Observation on the patient’s HIV Intake encounter with Concept:
“HIV Diagnosis Date” (I think this is more accurate that calling it “Test Date”, but we’d have to check)

Any other values that are on the current form and we can clearly map over
(eg. where there is an encounter_id linking to an intake or follow-up form, migrate test_location, test_location_other).

If any hiv_positive_p values = ‘f' or '?’, record entries in the data warning table to further look into these and
determine what to do with them

From here we will want to analyze what is remaining and not accounted for in the above and determine
what the scale is.  We could potentially just consider the rest to be voided data and exclude from the migration.

*/
    }

    @Override
    def void revert() {
        executeMysql("drop table if exists hivmigration_hiv_status")
    }
}
