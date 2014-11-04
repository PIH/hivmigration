package org.pih.hivmigration.export.query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pih.hivmigration.export.DB;
import org.pih.hivmigration.export.TestUtils;

import java.util.Collection;

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
}
