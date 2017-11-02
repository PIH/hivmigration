**Users**
* All current and historical users are migrated
* Usernames are migrated from email addresses by removing @xyz.com from them
* If duplicate usernames result, duplicate is created by appending “-userId” at the end
* Underlying persons are created with no gender or birthdate, a single person name, and email property
* Original password and salt are migrated, but are invalid in OpenMRS so will need to be reset as needed
* Existing users who have been disabled or deleted are set as retired
* **Test case**:  All foreign keyed data can connect to a user account

**Patients**
* All rows from hiv_demographics table are migrated
* Person created with gender, birthdate+estimated, actual created+creator
* Person Name created with first_name+first_name2->given name
* Patient record created for each person record
* HIVEMR-V1 identifier created at “Unknown Location” for each Patient with source patient id
* **Test case**:  hiv_demographics count comparisons

TODO
* pih_id, nif_id, national_id, phone_number, birth_place, accompagnateur_name
* Handle exclusion or annotation of test patients.  Consider treatment_status = ‘test’ and/or those with “test” names

**Infants**
* All rows from hiv_infants table are migrated
* Person, Person Name, Patient created with gender, birthdate, first_name, last_name with no actual creation data
* If infant has an associated mother_patient_id, Parent-> Child relationship is created with start_date = birth_date

TODO: 
* infant_code
* Is Parent->Child right, do we want to explicitly say Mother->Child?
* Make sure we handle remaining columns in hiv_infants table as needed

**HIV Program Enrollments**
* The below is the best set of assumptions we have made in order to adapt to data quality issues, and data model differences.  This is not necessarily consistent with current HIV EMR.
* Limits HIV program patients to those who have at least 1 encounter of type 'intake','followup','patient_contact','accompagnateur'
* Uses hiv_art_regimen_start_date view to get patient's ART start date, and uses this to determine whether they have any "On ART" state
* Uses hiv_demographics.treatment_status/treatment_status_date as primary determinant if the patient is currently enrolled and what outcome + date is
** died=died, lost/abandoned=lost, transferred_out=transferred_out, treatment_refused/treatment_stopped*=treatment_stopped
* Uses hiv_reg_outcome_helper_dates view as secondary determinant if patient has a particular outcome and outcome date
* Tries to determine various program dates if the patient has an outcome:
** outcome_date = treatment_status_date.
** If outcome_date is null or regimen_outcome_date is later, use outcome_date = regimen_outcome_date
** If outcome_date is still null, use 6 months after the last visit date as an estimate for outcome_date
** If for some reason the ART start date > outcome_date, set ART start date = outcome_date
* Program enrollment date is the earlier of min(encounter_date) and art_start_date
** If outcome_date < enrollment_date, set outcome_date = enrollment_date
* Creates 2 enrollments instead of 1 if:
** current_health_center and starting_health_center are both non-null, and not the same, and not equal to 42 (Non-ZL Site)
** health_center_transfer_date exists and is not before the enrollment date
* Otherwise, creates 1 enrollment
