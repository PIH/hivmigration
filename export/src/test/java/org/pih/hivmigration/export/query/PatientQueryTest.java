package org.pih.hivmigration.export.query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pih.hivmigration.common.HivStatusData;
import org.pih.hivmigration.common.code.HivStatus;
import org.pih.hivmigration.export.DB;
import org.pih.hivmigration.export.TestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PatientQueryTest {

	@Before
	public void beforeTest() throws Exception {
		DB.openConnection(TestUtils.getDatabaseCredentials());
	}

	@After
	public void afterTest() throws Exception {
		DB.closeConnection();
	}

	@Test
	public void shouldTestPatientQuery() throws Exception {
		Collection c = PatientQuery.getPatients().values();
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
		Collection c = PatientQuery.getIntakeEncounters().values();
		TestUtils.assertCollectionSizeMatchesQuerySize(c, "select count(encounter_id) from hiv_encounters where type = 'intake'");
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
				Assert.assertFalse("Patient " + pId + " has a historical positive HIV status, but this is not in the export", positivePats.contains(pId));
			}
		}
	}

	// TODO: Fill out this unit test to validate assumptions around which tables contain data for which encounter types
	@Test
	public void shouldIncludeOnlySpecifiedTablesForEncounters() throws Exception {
		List<String> queries = new ArrayList<String>();
		queries.add("select count(*) from hiv_followup_forms where encounter_id in (select encounter_id from hiv_encounters where type <> 'followup')");
		queries.add("select count(*) from hiv_intake_forms where encounter_id in (select encounter_id from hiv_encounters where type <> 'intake')");

		for (String query : queries) {
			Assert.assertEquals(0, DB.uniqueResult(query, Integer.class).intValue());
		}
	}
}
