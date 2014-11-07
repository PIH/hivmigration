package org.pih.hivmigration.export.query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pih.hivmigration.common.FollowupEncounter;
import org.pih.hivmigration.common.HivStatusData;
import org.pih.hivmigration.common.IntakeEncounter;
import org.pih.hivmigration.common.Patient;
import org.pih.hivmigration.common.code.HivStatus;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.export.DB;
import org.pih.hivmigration.export.TestUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PatientQueryTest {

	private static Map<Integer, Patient> patients;
	private static ListMap<Integer, IntakeEncounter> intakeEncounters;
	private static ListMap<Integer, FollowupEncounter> followupEncounters;

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
			if (d.getStatus() == HivStatus.NEGATIVE || d.getStatus() == HivStatus.UNKNOWN) {
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
	public void shouldTestOpportunisticInfections() throws Exception {
		Collection c = PatientQuery.getOpportunisticInfections().values();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(c, "hiv_exam_ois");
		TestUtils.assertAllPropertiesArePopulated(c);
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
