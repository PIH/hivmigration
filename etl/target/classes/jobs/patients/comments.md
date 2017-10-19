/**
 * The base patient is essentially mapped to the hiv_demographics table.  It contains the core demographic
 * data, patient identifiers, and non-encounter-based person attributes, like birthplace and phone number
 * 
 * We are excluding the following data from hiv_demographics
 *
 * No data: LAST_NAME2, GPS_LONGITUDE, GPS_LATITUDE, MAIDEN_NAME, COMMENTS,  CIVIL_STATUS, SOCIAL_STATUS, PROFESSION,
 *          EDUCATION, PARITY, LIVE_BIRTHS, BIRTH_DATES, EVALUATION_DATE, DATA_COLLECTED_BY
 *
 * Static or unused data:  CITIZENSHIP (ht), PATIENT_TYPE (pending)
 */