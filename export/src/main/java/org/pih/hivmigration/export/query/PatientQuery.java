package org.pih.hivmigration.export.query;

import org.pih.hivmigration.common.Address;
import org.pih.hivmigration.common.Allergy;
import org.pih.hivmigration.common.Contact;
import org.pih.hivmigration.common.Diagnosis;
import org.pih.hivmigration.common.FollowupEncounter;
import org.pih.hivmigration.common.HivStatusData;
import org.pih.hivmigration.common.IntakeEncounter;
import org.pih.hivmigration.common.LabTestOrder;
import org.pih.hivmigration.common.OpportunisticInfection;
import org.pih.hivmigration.common.PamEnrollment;
import org.pih.hivmigration.common.Patient;
import org.pih.hivmigration.common.PostnatalEncounter;
import org.pih.hivmigration.common.Pregnancy;
import org.pih.hivmigration.common.PreviousTreatment;
import org.pih.hivmigration.common.ResponsiblePerson;
import org.pih.hivmigration.common.SocioeconomicData;
import org.pih.hivmigration.common.SymptomGroup;
import org.pih.hivmigration.common.SystemStatus;
import org.pih.hivmigration.common.code.WhoStagingCriteria;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.export.DB;
import org.pih.hivmigration.export.JoinData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PatientQuery {

	/**
	 * The base patient object is essentially mapped to the hiv_demographics table.  It contains the core demographic
	 * data, patient identifiers, and non-encounter-based person attributes, like birthplace and phone number
	 * We are excluding the following data from hiv_demographics
	 *
	 * No data: LAST_NAME2, GPS_LONGITUDE, GPS_LATITUDE, MAIDEN_NAME, COMMENTS,  CIVIL_STATUS, SOCIAL_STATUS, PROFESSION, EDUCATION, PARITY, LIVE_BIRTHS, BIRTH_DATES, EVALUATION_DATE, DATA_COLLECTED_BY
	 * Static / unused data:  CITIZENSHIP (ht), PATIENT_TYPE (pending)
	 *
	 * TODO
	 * Included in HIV program enrollment data:  STARTING_HEALTH_CENTER, HEALTH_CENTER_TRANSFER_DATE, HEALTH_CENTER, TREATMENT_STATUS, TREATMENT_STATUS_DATE, TREATMENT_START_DATE (unsure how to use this)
	 * Not including DOES_NOT_MATCH as not really sure it is needed for migration
	 * Not including ZONE.  This should just go on the intake form if anywhere (it was only ever collected on v1 of the intake form, and not since).  Really part of address.
	 * Figure out what do do with zone and treatment_start_date
	 */
	public static Map<Integer, Patient> getPatients() {
		StringBuilder query = new StringBuilder();
		query.append("select	d.patient_id, d.pih_id, d.nif_id, d.national_id, d.first_name, d.first_name2, d.last_name, ");
		query.append("			d.gender, d.birth_date, d.birth_date_exact_p as birthdateEstimated, d.phone_number, ");
		query.append("			d.birth_place, d.agent as accompagnateur, d.patient_created_by, d.patient_created_date ");
		query.append("from		hiv_demographics d ");

		List<JoinData> joinData = new ArrayList<JoinData>();
		joinData.add(new JoinData("patient_id", "addresses", getAddresses()));
		joinData.add(new JoinData("patient_id", "pamEnrollment", getPamEnrollments()));
		joinData.add(new JoinData("patient_id", "pregnancies", getPregnancies()));
		joinData.add(new JoinData("patient_id", "postnatalEncounters", getPostnatalEncounters()));

		return DB.mapResult(query, Patient.class, joinData);
	}

	/**
	 * @return Map from patientId -> List of Addresses, with the first element of each List representing the current known address
	 *
	 * Rather than including "type" in the returned data, we are simply sorting by it, along with entry_date.  The nature of the triggers set up in the
	 * database, and our exclusive usage of entering "current" addresses means that we should just be able to sort by entry date desc and use the first record
	 * as the current address, but we add in the "type" of current vs. previous for good measure.
	 */
	public static ListMap<Integer, Address> getAddresses() {
		StringBuilder query = new StringBuilder();
		query.append("select	a.patient_id, a.entry_date, a.address, nvl(l.department, a.department) as department, ");
		query.append("			nvl(l.commune, a.commune) as commune, nvl(l.section_communale, a.section) as section, nvl(l.locality, a.locality) as locality ");
		query.append("from		hiv_addresses a, hiv_locality l ");
		query.append("where		a.locality_id = l.locality_id(+) ");
		query.append("order by	decode(a.type, 'current', 1, 0) desc, a.entry_date desc ");
		return DB.listMapResult(query, Address.class);
	}

	/**
	 * @return Map from patientId -> PamEnrollment for all patients who have had an enrollment in this program
	 *
	 * There are a handful of patients that have more than one entry in this table, but upon investigation they are duplicates on all fields, so we can safely do a 1:1 mapping to patient
	 */
	public static Map<Integer, PamEnrollment> getPamEnrollments() {
		StringBuilder query = new StringBuilder();
		query.append("select	patient_id, patient_tx_id as identifier, start_date, end_date ");
		query.append("from		hiv_course_of_tx ");
		return DB.mapResult(query, PamEnrollment.class);
	}

	/**
	 * @return Map from patientId -> List of Pregnancies for a given patient
	 *
	 * This includes all data from the hiv_pregnancy, hiv_pregnancy_exam tables.
	 * It also includes all data from hiv_encounters where type = 'infant_followup'
	 * The PostnatalEncounter Data may not be useful for import as there are only a handful recorded, almost all of them during a single week of October in 2007
	 */
	public static ListMap<Integer, Pregnancy> getPregnancies() {
		StringBuilder pregnancyQuery = new StringBuilder();
		pregnancyQuery.append("select	patient_id, pregnancy_id, last_period_date, expected_delivery_date, gravidity, parity, num_abortions, num_living_children, ");
		pregnancyQuery.append("			family_planning_method, post_outcome_family_planning, comments, outcome, outcome_date, outcome_location, outcome_method ");
		pregnancyQuery.append("from		hiv_pregnancy ");
		return DB.listMapResult(pregnancyQuery, Pregnancy.class);
	}

	/**
	 * @return Map from patientId -> List of PostnatalEncounters for a given patient
	 *
	 * This includes all data from hiv_pregnancy_exam and hiv_encounters where type = 'infant_followup'
	 * The PostnatalEncounter Data may not be useful for import as there are only a handful recorded, almost all of them during a single week of October in 2007
	 */
	public static ListMap<Integer, PostnatalEncounter> getPostnatalEncounters() {
		StringBuilder examQuery = new StringBuilder();
		examQuery.append("select	e.patient_id, e.encounter_date, 'Unspecified' as location, e.entered_by, e.entry_date, ");
		examQuery.append("			x.pregnancy_id, x.child_serostatus_test as childHivTestType, x.child_serostatus_result as childHivTestResult, x.child_status ");
		examQuery.append("from		hiv_encounters e, hiv_pregnancy_exam x ");
		examQuery.append("where		e.encounter_id = x.encounter_id(+) ");
		examQuery.append("and		e.type = 'infant_followup' ");
		return DB.listMapResult(examQuery, PostnatalEncounter.class);
	}

	/**
	 * @return Map from patientId to a List of IntakeEncounters, including all data that is entered on intake forms
	 *
	 * There are 29 examples (as of 11/4/14) of patients who have more than one intake encounter, so for now I'm making this a ListMap and we can figure it out as we go
	 */
	public static ListMap<Integer, IntakeEncounter> getIntakeEncounters() {
		StringBuilder query = new StringBuilder();
		query.append("select	e.patient_id, e.encounter_id, e.encounter_date, e.encounter_site as location, e.entered_by, e.entry_date, e.comments, ");
		query.append("			f.address, f.examining_doctor, f.recommendation as recommendations, f.previous_diagnoses, f.financial_aid_p as startFinancialAid, ");
		query.append("			f.continue_financial_aid_p as continueFinancialAid, f.nutritional_aid_p as startNutritionalAid, f.form_version, ");
		query.append("			x.hospitalized_at_diagnosis_p as hospitalizedAtDiagnosis, exam.presenting_history as presentingComplaint, ");
		query.append("			exam.diagnosis as differentialDiagnosis, exam.comments as physicalExamComments ");
		query.append("from		hiv_encounters e, hiv_intake_forms f, hiv_intake_extra x, hiv_exams exam ");
		query.append("where		e.encounter_id = f.encounter_id(+) ");
		query.append("and		e.encounter_id = x.encounter_id(+) ");
		query.append("and		e.encounter_id = exam.encounter_id(+) ");
		query.append("and		e.type = 'intake'");

		List<JoinData> joinData = new ArrayList<JoinData>();
		joinData.add(new JoinData("patient_id", "allergies", getAllergies()));
		joinData.add(new JoinData("patient_id", "contacts", getContacts()));
		joinData.add(new JoinData("patient_id", "diagnoses", getDiagnoses()));
		joinData.add(new JoinData("patient_id", "previousTreatments", getPreviousTreatments()));
		joinData.add(new JoinData("patient_id", "socioeconomicData", getSocioeconomicData()));
		joinData.add(new JoinData("patient_id", "hivStatusData", getHivStatusData()));
		joinData.add(new JoinData("encounter_id", "systemStatuses", getSystemStatuses()));
		joinData.add(new JoinData("encounter_id", "whoStagingCriteria", getWhoStagingCriteria()));
		joinData.add(new JoinData("encounter_id", "responsiblePerson", getResponsiblePersonData()));
		joinData.add(new JoinData("encounter_id", "opportunisticInfections", getOpportunisticInfections()));
		joinData.add(new JoinData("encounter_id", "symptomGroups", getSymptomGroups()));
		joinData.add(new JoinData("encounter_id", "labTestOrders", getLabTestOrders()));

		return DB.listMapResult(query, IntakeEncounter.class, joinData);
	}

	/**
	 * @return Map from patientId to a List of FollowupEncounter, including all data that is entered on intake forms
	 */
	public static ListMap<Integer, FollowupEncounter> getFollowupEncounters() {
		StringBuilder query = new StringBuilder();
		query.append("select	e.patient_id, e.encounter_id, e.encounter_date, e.encounter_site as location, e.entered_by, e.entry_date, e.comments, ");
		query.append("			f.examining_doctor, f.recommendations, f.progress, f.well_followed_p as wellFollowed, f.financial_aid_p as startFinancialAid, ");
		query.append("			f.continue_financial_aid_p as continueFinancialAid, f.med_toxicity_p as med_toxicity, f.med_toxicity_comments, f.form_version, ");
		query.append("			exam.presenting_history as presentingComplaint, exam.comments as physicalExamComments ");
		query.append("from		hiv_encounters e, hiv_followup_forms f, hiv_exams exam ");
		query.append("where		e.encounter_id = f.encounter_id(+) ");
		query.append("and		e.encounter_id = exam.encounter_id(+) ");
		query.append("and		e.type = 'followup'");

		List<JoinData> joinData = new ArrayList<JoinData>();
		joinData.add(new JoinData("encounter_id", "responsiblePerson", getResponsiblePersonData()));
		joinData.add(new JoinData("encounter_id", "opportunisticInfections", getOpportunisticInfections()));
		joinData.add(new JoinData("encounter_id", "symptomGroups", getSymptomGroups()));
		joinData.add(new JoinData("encounter_id", "labTestOrders", getLabTestOrders()));

		return DB.listMapResult(query, FollowupEncounter.class, joinData);
	}

	/**
	 * @return Map from patientId to a List of Allergies.  Note, many of these Allergies simply have an allergen of "aucun", which we'll need to interpret as no known allergies
	 */
	public static ListMap<Integer, Allergy> getAllergies() {
		StringBuilder query = new StringBuilder();
		query.append("select	patient_id, nvl(inn, other_reason) as allergen, allergy_date, type_of_reaction ");
		query.append("from		hiv_allergies ");
		return DB.listMapResult(query, Allergy.class);
	}

	/**
	 * @return Map from patientId to a List of Contacts.
	 */
	public static ListMap<Integer, Contact> getContacts() {
		StringBuilder query = new StringBuilder();
		query.append("select	patient_id, contact_name, relationship, birth_date, age, hiv_status, ");
		query.append("			clinic_p as followedInAClinicForHivCare, clinic_name as nameOfClinic, ");
		query.append("			referred_tr_p as referredForHivTest, referred_clinic as nameOfReferralClinic, deceased_p as deceased ");
		query.append("from		hiv_contacts ");
		return DB.listMapResult(query, Contact.class);
	}

	/**
	 * @return Map from patientId to a List of Diagnosis.  Note, these indicate both diagnoses that are present and absent depending on the modifier
	 */
	public static ListMap<Integer, Diagnosis> getDiagnoses() {
		StringBuilder query = new StringBuilder();
		query.append("select	p.patient_id,  d.diagnosis_eng as diagnosisCoded, diagnosis_other as diagnosisNonCoded, ");
		query.append("			p.present_p as present, p.diagnosis_date, p.diagnosis_comments ");
		query.append("from		hiv_patient_diagnoses p, hiv_diagnoses d ");
		query.append("where		p.diagnosis_id = d.diagnosis_id(+) ");
		return DB.listMapResult(query, Diagnosis.class);
	}

	/**
	 * @return Map from patientId to a List of PreviousTreatments
	 */
	public static ListMap<Integer, PreviousTreatment> getPreviousTreatments() {
		StringBuilder query = new StringBuilder();
		query.append("select	patient_id, inn as treatmentCoded, treatment_other as treatmentNonCoded, ");
		query.append("			start_date, end_date, treatment_outcome as outcome ");
		query.append("from		hiv_previous_exposures ");
		return DB.listMapResult(query, PreviousTreatment.class);
	}

	/**
	 * @return Map from patientId to the socioeconomic data recorded on the intake form
	 */
	public static Map<Integer, SocioeconomicData> getSocioeconomicData() {
		StringBuilder query = new StringBuilder();
		query.append("select 	s.patient_id, s.civil_status, s.other_sexual_partners_p as has_other_sexual_partners, s.other_sexual_partners, ");
		query.append("			s.partners_same_town_p as partnersInSameTown, s.partners_other_town, ");
		query.append("			s.residences, s.primary_activity, s.nutritional_evaluation, s.num_people_in_house, s.num_rooms_in_house, ");
		query.append("			s.type_of_roof, s.type_of_floor, s.latrine_p as latrinePresent, s.radio_p as radioPresent, s.education, ");
		query.append("			x.education_years, x.method_of_transport, x.time_of_transport, x.walking_time_to_clinic, x.smokes_p as smokes, ");
		query.append("			x.smoking_years, x.num_cigarretes_per_day, x.num_days_alcohol_per_week, x.wine_per_day, x.beer_per_day, x.drinks_per_day, ");
		query.append("			x.arrival_method_other as arrivalOtherMethod ");
		query.append("from		hiv_socioeconomics s, hiv_socioeconomics_extra x ");
		query.append("where		s.patient_id = x.patient_id(+) ");
		return DB.mapResult(query, SocioeconomicData.class);
	}

	/**
	 * @return Map from patientId to hiv status data recorded on the intake form
	 *
	 * HIV_HIV_STATUS HAS LINKS TO BOTH ENCOUNTER AND PATIENT, BUT ALL LINKED ENCOUNTERS ARE INTAKE, AND NO ENCOUNTER IMPLIES INTAKE
	 * IT'S USAGE IS SUCH THAT ANY EDIT TO THESE FIELDS ON THE INTAKE FORM WILL "AUDIT" THE OLDER VALUE, BY SETTING THE "TYPE" TO "PREVIOUS"
	 * THE PLAN IS TO MIGRATE ONLY THE MOST RECENT ENTRY FOR EACH DISTINCT PATIENT IN THIS TABLE,
	 * WHICH SHOULD REPRESENT THE LATEST INTAKE FORM UPDATE AND THE MOST ACCURATE INFORMATION, WHETHER ENTERED ORIGINALLY OR IN A CHART REVIEW, ETC
	 */
	public static Map<Integer, HivStatusData> getHivStatusData() {
		StringBuilder query = new StringBuilder();
		query.append("select	patient_id, hiv_positive_p as status, status_date, date_unknown_p as dateUnknown, ");
		query.append("			test_location as testLocationCoded, test_location_other as testLocationNonCoded, entered_date as entryDate, entered_by ");
		query.append("from		hiv_hiv_status ");
		query.append("order by	entered_date asc");
		return DB.mapResult(query, HivStatusData.class);
	}

	/**
	 * @return Map from encounterId to a List of SystemStatus which represent an evaluation of body systems at intake
	 */
	public static ListMap<Integer, SystemStatus> getSystemStatuses() {
		StringBuilder query = new StringBuilder();
		query.append("select	encounter_id, system, condition ");
		query.append("from		hiv_exam_system_status ");
		return DB.listMapResult(query, SystemStatus.class);
	}

	/**
	 * @return Map from encounterId to a List of OpportunisticInfections
	 */
	public static ListMap<Integer, OpportunisticInfection> getOpportunisticInfections() {
		StringBuilder query = new StringBuilder();
		query.append("select	encounter_id, oi, comments ");
		query.append("from		hiv_exam_ois ");
		return DB.listMapResult(query, OpportunisticInfection.class);
	}

	/**
	 * @return Map from encounterId to a List of WhoStagingCriteria
	 */
	public static ListMap<Integer, WhoStagingCriteria> getWhoStagingCriteria() {
		StringBuilder query = new StringBuilder();
		query.append("select	encounter_id, criterium ");
		query.append("from		hiv_exam_who_staging_criteria ");
		return DB.enumResult(query, WhoStagingCriteria.class);
	}

	/**
	 * @return Map from encounterId to a ResponsiblePerson
	 */
	public static Map<Integer, ResponsiblePerson> getResponsiblePersonData() {
		StringBuilder query = new StringBuilder();
		query.append("select	encounter_id, (responsible_first_name || decode(responsible_first_name2, null, '', (' ' || responsible_first_name2))) as firstName, ");
		query.append(" 			responsible_last_name as lastName, responsible_pih_id as pihId, responsible_relation as relationship ");
		query.append("from		hiv_intake_extra ");
		query.append("where		(responsible_first_name is not null or responsible_first_name2 is not null or responsible_last_name is not null ");
		query.append("or		responsible_pih_id is not null or responsible_relation is not null) ");
		return DB.mapResult(query, ResponsiblePerson.class);
	}

	/**
	 * @return Map from encounterId to a List of SymptomGroups
	 */
	public static ListMap<Integer, SymptomGroup> getSymptomGroups() {
		StringBuilder query = new StringBuilder();
		query.append("select 	encounter_id, symptom, result as symptomPresent, symptom_date, duration, duration_unit, symptom_comment ");
		query.append("from		hiv_exam_symptoms ");
		return DB.listMapResult(query, SymptomGroup.class);
	}

	/**
	 * @return Map from encounterId to a List of LabTestOrder
	 */
	public static ListMap<Integer, LabTestOrder> getLabTestOrders() {
		StringBuilder query = new StringBuilder();
		query.append("select	encounter_id, test as testCoded, test_other as testNonCoded ");
		query.append("from		hiv_ordered_lab_tests ");
		return DB.listMapResult(query, LabTestOrder.class);
	}
}
