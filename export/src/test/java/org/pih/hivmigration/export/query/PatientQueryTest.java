package org.pih.hivmigration.export.query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pih.hivmigration.common.CervicalCancerEncounter;
import org.pih.hivmigration.common.FollowupEncounter;
import org.pih.hivmigration.common.HivStatusData;
import org.pih.hivmigration.common.IntakeEncounter;
import org.pih.hivmigration.common.LabResultEncounter;
import org.pih.hivmigration.common.LabTestResult;
import org.pih.hivmigration.common.NutritionalEvaluationEncounter;
import org.pih.hivmigration.common.Patient;
import org.pih.hivmigration.common.PatientContactEncounter;
import org.pih.hivmigration.common.PregnancyDataEntryTransaction;
import org.pih.hivmigration.common.code.SimpleLabResult;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.export.DB;
import org.pih.hivmigration.export.TestUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatientQueryTest {

	private static Map<Integer, Patient> patients;
	private static ListMap<Integer, IntakeEncounter> intakeEncounters;
	private static ListMap<Integer, FollowupEncounter> followupEncounters;
	private static ListMap<Integer, PatientContactEncounter> patientContactEncounters;
	private static ListMap<Integer, CervicalCancerEncounter> cervicalCancerEncounters;
	private static ListMap<Integer, NutritionalEvaluationEncounter> nutritionalEvaluationEncounters;
	private static ListMap<Integer, LabResultEncounter> labResultEncounters;
	private static ListMap<Integer, PregnancyDataEntryTransaction> pregnancyDataEntryTransactions;

	@Before
	public void beforeTest() throws Exception {
		DB.openConnection(TestUtils.getDatabaseCredentials());
	}

	@After
	public void afterTest() throws Exception {
		DB.closeConnection();
	}

	@AfterClass
	public static void clearCaches() {
		patients = null;
		intakeEncounters = null;
		followupEncounters = null;
		patientContactEncounters = null;
		cervicalCancerEncounters = null;
		nutritionalEvaluationEncounters = null;
		labResultEncounters = null;
		pregnancyDataEntryTransactions = null;
	}

	protected Collection<Patient> getPatients() {
		if (patients == null) {
			patients = PatientQuery.getPatients();
		}
		return patients.values();
	}

	protected ListMap<Integer, IntakeEncounter> getIntakeEncounters() {
		if (intakeEncounters == null) {
			intakeEncounters = PatientQuery.getIntakeEncounters();
		}
		return intakeEncounters;
	}

	protected ListMap<Integer, FollowupEncounter> getFollowupEncounters() {
		if (followupEncounters == null) {
			followupEncounters = PatientQuery.getFollowupEncounters();
		}
		return followupEncounters;
	}

	protected ListMap<Integer, PatientContactEncounter> getPatientContactEncounters() {
		if (patientContactEncounters == null) {
			patientContactEncounters = PatientQuery.getPatientContactEncounters();
		}
		return patientContactEncounters;
	}

	protected ListMap<Integer, CervicalCancerEncounter> getCervicalCancerEncounters() {
		if (cervicalCancerEncounters == null) {
			cervicalCancerEncounters = PatientQuery.getCervicalCancerEncounters();
		}
		return cervicalCancerEncounters;
	}

	protected ListMap<Integer, NutritionalEvaluationEncounter> getNutritionalEvaluationEncounters() {
		if (nutritionalEvaluationEncounters == null) {
			nutritionalEvaluationEncounters = PatientQuery.getNutritionalEvaluationEncounters();
		}
		return nutritionalEvaluationEncounters;
	}

	protected ListMap<Integer, LabResultEncounter> getLabResultEncounters() {
		if (labResultEncounters == null) {
			labResultEncounters = PatientQuery.getLabResultEncounters();
		}
		return labResultEncounters;
	}

	protected ListMap<Integer, PregnancyDataEntryTransaction> getPregnancyDataEntryTransactions() {
		if (pregnancyDataEntryTransactions == null) {
			pregnancyDataEntryTransactions = PatientQuery.getPregnancyDataEntryTransactions();
		}
		return pregnancyDataEntryTransactions;
	}

	@Test
	public void shouldTestPatientQuery() throws Exception {
		Collection c = getPatients();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_demographics");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestAddressQuery() throws Exception {
		Collection c = PatientQuery.getAddresses().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_addresses");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestPamQuery() throws Exception {
		Collection c = PatientQuery.getPamEnrollments().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(distinct(patient_id)) from hiv_course_of_tx");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestPregnancyQuery() throws Exception {
		Collection c = PatientQuery.getPregnancies().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_pregnancy");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestPostnatalEncounterQuery() throws Exception {
		Collection c = PatientQuery.getPostnatalEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'infant_followup'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestIntakeEncounters() throws Exception {
		Collection c = getIntakeEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'intake'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestFollowupEncounters() throws Exception {
		Collection c = getFollowupEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'followup'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestPatientContactEncounters() throws Exception {
		Collection c = getPatientContactEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'patient_contact'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestCervicalCancerEncounters() throws Exception {
		Collection c = getCervicalCancerEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'cervical_cancer'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestNutritionalEvaluationEncounters() throws Exception {
		Collection c = getNutritionalEvaluationEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'food_study'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestLabResultEncounters() throws Exception {
		Collection c = getLabResultEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'lab_result'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestPregnancyDataEntryTransactions() throws Exception {
		Collection c = getPregnancyDataEntryTransactions().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'pregnancy'");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestEncounterProperties() throws Exception {
		int expectedComments = DB.uniqueResult("select count(*) from hiv_encounters where comments is not null", Integer.class);

		// TODO: Uncomment and update this when we figure out what to do with "note" encounters
		//TestUtils.assertAllValuesAreJoinedToEncounters(expectedComments, "comments", getIntakeEncounters(), getFollowupEncounters(), getPatientContactEncounters());
	}

	@Test
	public void shouldTestAllergies() throws Exception {
		Collection c = PatientQuery.getAllergies().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_allergies");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestContacts() throws Exception {
		Collection c = PatientQuery.getContacts().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_contacts");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestDiagnoses() throws Exception {
		Collection c = PatientQuery.getDiagnoses().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_patient_diagnoses");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestPreviousTreatments() throws Exception {
		Collection c = PatientQuery.getPreviousTreatments().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_previous_exposures");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestSocioeconomicData() throws Exception {
		Collection c = PatientQuery.getSocioeconomicData().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_socioeconomics");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestHivStatusData() throws Exception {
		Collection c = PatientQuery.getHivStatusData().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(distinct(patient_id)) from hiv_hiv_status");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldNotReturnAnyHivNegativeStatusesIncorrectly() throws Exception {
		List<Integer> positivePats = DB.listResult("select distinct patient_id from hiv_hiv_status where hiv_positive_p = 't'", Integer.class);
		Map<Integer, HivStatusData> data = PatientQuery.getHivStatusData();
		for (Integer pId : data.keySet()) {
			HivStatusData d = data.get(pId);
			if (d.getStatus() == SimpleLabResult.NEGATIVE || d.getStatus() == SimpleLabResult.UNKNOWN) {
				Assert.assertFalse("Patient " + pId + " has a historical positive HIV status but this is not in the export", positivePats.contains(pId));
			}
		}
	}

	@Test
	public void shouldTestSystemStatusData() throws Exception {
		Collection c = PatientQuery.getSystemStatuses().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_exam_system_status");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestWhoStagingCriteria() throws Exception {
		Collection c = PatientQuery.getWhoStagingCriteria().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_exam_who_staging_criteria");
		TestUtils.assertAllPropertiesArePopulated(c);
	}

	@Test
	public void shouldTestHivIntakeExtraData() throws Exception {
		Collection<List<IntakeEncounter>> intakes = getIntakeEncounters().values();
		Collection<List<FollowupEncounter>> followups = getFollowupEncounters().values();
		{
			int numResponsibleFoundOnIntake = TestUtils.getNonNullPropertiesFoundInCollection(intakes, "responsiblePerson");
			int numResponsibleFoundOnFollowup = TestUtils.getNonNullPropertiesFoundInCollection(followups, "responsiblePerson");
			StringBuilder q = new StringBuilder();
			q.append("select count(*) from hiv_intake_extra where responsible_first_name is not null or responsible_first_name2 is not null ");
			q.append("or responsible_last_name is not null or responsible_pih_id is not null or responsible_relation is not null ");
			Assert.assertEquals(DB.uniqueResult(q.toString(), Integer.class).intValue(), numResponsibleFoundOnIntake + numResponsibleFoundOnFollowup);
		}
		{
			int numFound = TestUtils.getNonNullPropertiesFoundInCollection(intakes, "hospitalizedAtDiagnosis");
			String q ="select count(*) from hiv_intake_extra where hospitalized_at_diagnosis_p is not null ";
			Assert.assertEquals(DB.uniqueResult(q, Integer.class).intValue(), numFound);
		}
	}

	@Test
	public void shouldTestHivExamData() throws Exception {
		Collection<List<IntakeEncounter>> intakes = getIntakeEncounters().values();
		Collection<List<FollowupEncounter>> followups = getFollowupEncounters().values();
		{
			int found = TestUtils.getNonNullPropertiesFoundInCollection(intakes, "presentingComplaint") + TestUtils.getNonNullPropertiesFoundInCollection(followups, "presentingComplaint");
			int expected = DB.uniqueResult("select count(*) from hiv_exams where presenting_history is not null", Integer.class);
			Assert.assertEquals(expected, found);
		}
		{
			int found = TestUtils.getNonNullPropertiesFoundInCollection(intakes, "physicalExamComments") + TestUtils.getNonNullPropertiesFoundInCollection(followups, "physicalExamComments");
			int expected = DB.uniqueResult("select count(*) from hiv_exams where comments is not null", Integer.class);
			Assert.assertEquals(expected, found);
		}
		{
			int found = TestUtils.getNonNullPropertiesFoundInCollection(intakes, "differentialDiagnosis");
			int expected = DB.uniqueResult("select count(*) from hiv_exams where diagnosis is not null", Integer.class);
			Assert.assertEquals(expected, found);
		}
		{
			int found = DB.uniqueResult("select count(*) from hiv_exams where presenting_complaint is not null", Integer.class);
			Assert.assertEquals(0, found);
		}
	}

	@Test
	public void shouldTestHivExamExtraData() throws Exception {
		{
			int expected = DB.uniqueResult("select count(*) from hiv_exam_extra where last_period_date is not null", Integer.class);
			TestUtils.assertAllValuesAreJoinedToEncounters(expected, "lastPeriodDate", getIntakeEncounters(), getFollowupEncounters(), getCervicalCancerEncounters());
		}
		{
			int expected = DB.uniqueResult("select count(*) from hiv_exam_extra where expected_delivery_date is not null", Integer.class);
			TestUtils.assertAllValuesAreJoinedToEncounters(expected, "expectedDeliveryDate", getIntakeEncounters(), getFollowupEncounters(), getPregnancyDataEntryTransactions());
		}
		{
			int expected = DB.uniqueResult("select count(*) from hiv_exam_extra where pregnant_p is not null", Integer.class);
			TestUtils.assertAllValuesAreJoinedToEncounters(expected, "pregnant", getIntakeEncounters(), getFollowupEncounters(), getPregnancyDataEntryTransactions());
		}
		Map<String, String> clinicalProperties = new HashMap<String, String>();
		clinicalProperties.put("mothersFirstName", "mothers_first_name");
		clinicalProperties.put("mothersLastName", "mothers_last_name");
		clinicalProperties.put("postTestCounseling", "post_test_counseling_p");
		clinicalProperties.put("partnerReferralStatus", "partner_referred_for_tr_p");
		clinicalProperties.put("nextExamDate", "next_exam_date");
		clinicalProperties.put("whoStage", "who_stage");
		clinicalProperties.put("mainActivityBefore", "main_activity_before");
		clinicalProperties.put("mainActivityHowNow", "main_activity_how_now");
		clinicalProperties.put("otherActivitiesBefore", "other_activities_before");
		clinicalProperties.put("otherActivitiesHowNow", "other_activities_how_now");
		clinicalProperties.put("oiNow", "oi_now_p");
		clinicalProperties.put("planExtra", "plan_extra");

		for (String property : clinicalProperties.keySet()) {
			int expected = DB.uniqueResult("select count(*) from hiv_exam_extra where " + clinicalProperties.get(property) + " is not null", Integer.class);
			TestUtils.assertAllValuesAreJoinedToEncounters(expected, property, getIntakeEncounters(), getFollowupEncounters());
		}
	}

	@Test
	public void shouldTestOpportunisticInfections() throws Exception {
		Collection c = PatientQuery.getOpportunisticInfections().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_exam_ois");
		TestUtils.assertAllPropertiesArePopulated(c);
		TestUtils.assertAllValuesAreJoinedToEncounters(c.size(), "opportunisticInfections", getIntakeEncounters(), getFollowupEncounters());
	}

	@Test
	public void shouldTestSymptomGroups() throws Exception {
		Collection c = PatientQuery.getSymptomGroups().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_exam_symptoms");
		TestUtils.assertAllPropertiesArePopulated(c);
		TestUtils.assertAllValuesAreJoinedToEncounters(c.size(), "symptomGroups", getIntakeEncounters(), getFollowupEncounters());
	}

	@Test
	public void shouldTestLabTestOrders() throws Exception {
		Collection c = PatientQuery.getLabTestOrders().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_ordered_lab_tests");
		TestUtils.assertAllPropertiesArePopulated(c);
		TestUtils.assertAllValuesAreJoinedToEncounters(c.size(), "labTestOrders", getIntakeEncounters(), getFollowupEncounters());
	}

	@Test
	public void shouldTestGenericOrders() throws Exception {
		Collection c = PatientQuery.getGenericOrders().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_ordered_other");
		TestUtils.assertAllPropertiesArePopulated(c);
		TestUtils.assertAllValuesAreJoinedToEncounters(c.size(), "genericOrders", getIntakeEncounters(), getFollowupEncounters());
	}

	@Test
	public void shouldTestLabTestResults() throws Exception {
		ListMap<Integer, LabTestResult> resultMap = PatientQuery.getLabTestResults();
		TestUtils.assertCollectionSizeMatchesQuerySize(resultMap.values(), "select count(*) from hiv_exam_lab_results where result is not null");
		TestUtils.assertAllPropertiesArePopulated(resultMap.values());
		TestUtils.assertAllValuesAreJoinedToEncounters(resultMap.size(), "labResults", getIntakeEncounters(), getFollowupEncounters(),
				getPatientContactEncounters(), getCervicalCancerEncounters(), getNutritionalEvaluationEncounters(), getLabResultEncounters());

		// TODO: Add more comprehensive testing of expected test results, datypes, etc
	}

	@Test
	public void shouldIncludeOnlySpecifiedTablesForIntakeEncounters() throws Exception {
		TestUtils.assertEncounterDataOnlyIn("intake",
				"HIV_HIV_STATUS", "HIV_INTAKE_FORMS", "HIV_INTAKE_EXTRA", "HIV_EXAM_SYSTEM_STATUS",
				"HIV_EXAM_WHO_STAGING_CRITERIA",

				"HIV_DATA_AUDIT_ENTRY", "HIV_EXAMS", "HIV_EXAM_EXTRA", "HIV_EXAM_LAB_RESULTS", "HIV_EXAM_OIS",
				"HIV_EXAM_SYMPTOMS",  "HIV_EXAM_VITAL_SIGNS", "HIV_OBSERVATIONS", "HIV_ORDERED_LAB_TESTS",
				"HIV_ORDERED_OTHER", "HIV_REGIMES", "HIV_TB_STATUS");
	}

	@Test
	public void shouldIncludeOnlySpecifiedTablesForFollowupEncounters() throws Exception {
		TestUtils.assertEncounterDataOnlyIn("followup",
				"HIV_FOLLOWUP_FORMS", "HIV_INTAKE_EXTRA",

				"HIV_DATA_AUDIT_ENTRY", "HIV_EXAMS", "HIV_EXAM_EXTRA", "HIV_EXAM_LAB_RESULTS", "HIV_EXAM_OIS",
				"HIV_EXAM_SYMPTOMS", "HIV_EXAM_VITAL_SIGNS", "HIV_OBSERVATIONS",
				"HIV_ORDERED_LAB_TESTS", "HIV_ORDERED_OTHER", "HIV_TB_STATUS", "HIV_ENCOUNTERS"
		);
	}
}
