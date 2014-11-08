hivmigration
============


# THESE REPRESENT ALL OF THE TABLES WITH DATA WE NEED TO EXPORT AND MIGRATE

##### TABLES THAT NEED FURTHER INVESTIGATION #####

# ACCOMPAGNATEUR:  AS OF 10/30/2014, NO DATA IN THESE TABLES
HIV_ACCOMPAGNATEUR
HIV_PATIENT_ACCOMPAGNATEUR

# DOUBLE CHECK WITH LOUISE ON THIS, BUT I BELIEVE THIS IS NO LONGER NEEDED. SIMPLY KEEPS TRACK OF WHICH PATIENTS ARE ANLAP AND WHAT GROUP THEY ARE ASSIGNED TO
HIV_ANLAP_PATIENTS

# DELETED PATIENTS.  I WOULD THINK ARCHIVING THIS IS SUFFICIENT
HIV_DELETED_PATIENTS

# TABLE THAT SIMPLY LINKS A PATIENT_ID WITH A MERGED PATIENT_ID, SO THAT IF SOMEONE IS LOOKING UP BY PATIENT ID AND THAT PATIENT HAS BEEN MERGED INTO ANOTHER, IT CAN BE FOUND.  KEEP?
HIV_MERGE_ARCHIVE

# OLD MESSAGES AND ERROR REPORTS.  CAN WE JUST GET THESE LOOKED AT AND ADDRESSED PRIOR TO MIGRATION, SINCE THEY ARE GENERALLY NO LONGER USED?
HIV_ERRORS
HIV_MESSAGES

# TABLE THAT KEEPS TRACK OF WHAT DATA QUALITY LISTS PATIENTS ARE SHOWING UP ON, AND ENABLES REMOVING THEM FROM THOSE LISTS AFTER REVIEW.  RECOMMEND NOT TO MIGRATE.
HIV_PATIENT_LIST_STATUS

# TABLE THAT KEEPS TRACK OF HISTORICAL REPORT EVALUATIONS AND ALLOWS FOR DOWNLOADING THE RESULTS
HIV_REPORT_REQUEST


###### TODO ######


###### METADATA / LOOKUP TABLES #####

HIV_INSTITUTIONS
HIV_LABORATORIES
HIV_LAB_TESTS
HIV_PRODUCTS
HIV_PRODUCT_CATEGORY
HIV_STANDARD_REGIMES
HIV_STANDARD_REGIME_DRUGS


###### PATIENT TABLES ######

# MANY-TO-ONE ENCOUNTER-LESS OBS
HIV_REGIME_COMMENTS

# MANY-TO-ONE BOOK-KEEPING TABLES KEEPING TRACK OF WHETHER PATIENT HAD A DATA AUDIT DONE, WHAT FIELDS CHANGED DURING THE AUDIT, COMMENTS, AND STATUS OF AUDIT
HIV_DATA_AUDIT
HIV_DATA_AUDIT_ENTRY

# MANY-TO-ONE PATIENT TRACKING FORMS
HIV_DATA_AUDIT_TRACKING_FORMS


###### ENCOUNTER TABLES ######

HIV_ENCOUNTERS

# Clinical Encounter Only
HIV_EXAM_SYMPTOMS
HIV_ORDERED_LAB_TESTS
HIV_ORDERED_OTHER


# OTHER COMBINATION
"accompagnateur", "HIV_DATA_AUDIT_ENTRY", "HIV_EXAM_LAB_RESULTS", "HIV_OBSERVATIONS"
"anlap_lab_result", "HIV_LAB_RESULTS"
"anlap_vital_signs", "HIV_EXAM_VITAL_SIGNS"
"cervical_cancer", "HIV_EXAM_EXTRA", "HIV_EXAM_LAB_RESULTS", "HIV_OBSERVATIONS"
"food_study", "HIV_EXAM_LAB_RESULTS", "HIV_EXAM_VITAL_SIGNS", "HIV_OBSERVATIONS"
"food_support", "HIV_EXAM_LAB_RESULTS", "HIV_OBSERVATIONS"
"hop_abstraction", "HIV_OBSERVATIONS", "HIV_TB_STATUS"
"lab_result", "HIV_DATA_AUDIT_ENTRY", "HIV_EXAM_LAB_RESULTS", "HIV_LAB_RESULTS"
"not_specified", "HIV_EXAM_VITAL_SIGNS"
"note", "HIV_EXAM_LAB_RESULTS"
"observation", "HIV_EXAM_VITAL_SIGNS"
"patient_contact", "HIV_DATA_AUDIT_ENTRY", "HIV_EXAM_LAB_RESULTS", "HIV_EXAM_VITAL_SIGNS"
"pregnancy", "HIV_EXAM_EXTRA", "HIV_OBSERVATIONS"
"regime", "HIV_EXAM_LAB_RESULTS", "HIV_OBSERVATIONS", "HIV_REGIMES"

"HIV_DATA_AUDIT_ENTRY", "accompagnateur", "followup", "intake", "lab_result", "patient_contact"
"HIV_EXAM_LAB_RESULTS", "accompagnateur", "cervical_cancer", "followup", "food_study", "food_support", "intake", "lab_result", "note", "patient_contact", "regime"
"HIV_OBSERVATIONS", "accompagnateur", "cervical_cancer", "followup", "food_study", "food_support", "hop_abstraction", "intake", "pregnancy", "regime"
"HIV_LAB_RESULTS", "anlap_lab_result", "lab_result"
"HIV_EXAM_VITAL_SIGNS", "anlap_vital_signs", "followup", "food_study", "intake", "not_specified", "observation", "patient_contact"
"HIV_EXAM_EXTRA", "cervical_cancer", "followup", "intake", "pregnancy"
"HIV_ENCOUNTERS", "followup", "lab_result", "note"
"HIV_TB_STATUS", "followup", "hop_abstraction", "intake"
"HIV_REGIMES", "intake", "regime"






# LINKED TO BOTH ENCOUNTER AND PATIENT.  WHEN ONLY PATIENT LINK EXISTS, IMPLIED TO BE FROM INTAKE
HIV_TB_STATUS

# ONE-TO-ONE WITH ADDITIONAL DATA
HIV_INTAKE_EXTRA
HIV_EXAM_EXTRA

# MANY-TO-ONE ENCOUNTER DATA
HIV_EXAM_LAB_RESULTS

HIV_EXAM_VITAL_SIGNS
HIV_LAB_RESULTS
HIV_OBSERVATIONS
HIV_REGIMES

# NOT LINKED TO ENCOUNTER DIRECTLY, BUT ONLY COLLECTED VIA HOP_ABSTRACTION ENCOUNTER.  patient_id, obs_name, obs_value, entry_date, entered_by
# HOSPITALIZED_AT_ART_INITIATION (t/f/not_specified) and CHART_NOT_FOUND (t)
HIV_PATIENT_OBS

###### EXPOSED INFANTS, PCR TESTING, SOCIAL SUPPORT ######
HIV_INFANTS
HIV_LAB_TRACKING
HIV_PCR_TESTS
HIV_PCR_TRACKING
HIV_SOCIAL_SUPPORT
HIV_SOCIAL_SUPPORT_TYPE

###### REPORTING AGGREGATE DATA ######
HIV_REPORT_INDICATOR_VALUE

HIV_REPORT_FREQUENCY
HIV_REPORT_GROUP
HIV_REPORT_GROUP_TEMPLATE
HIV_REPORT_INDICATOR
HIV_REPORT_INDICATOR_GROUP

###### SURVEILLANCE ######
HIV_SURVEILLANCE_LOG
HIV_SURVEILLANCE_LOG_ENTRY
HIV_SURVEILLANCE_NEW