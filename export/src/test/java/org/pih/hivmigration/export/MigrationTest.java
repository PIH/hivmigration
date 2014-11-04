package org.pih.hivmigration.export;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pih.hivmigration.common.Address;
import org.pih.hivmigration.common.PamEnrollment;
import org.pih.hivmigration.common.Patient;
import org.pih.hivmigration.common.User;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.export.query.PatientQuery;
import org.pih.hivmigration.export.query.UserQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MigrationTest {

	@Before
	public void beforeTest() throws Exception {
		DB.openConnection(TestUtils.getDatabaseCredentials());
	}

	@After
	public void afterTest() throws Exception {
		DB.closeConnection();
	}

	@Test
	public void shouldAnalyzeTable() throws Exception {
		String table = "HIV_COURSE_OF_TX";
		System.out.println("*********************");
		System.out.println(table);
		System.out.println("*********************");
		for (TableColumn column : DB.getAllColumns(table)) {
			TableColumnBreakdown breakdown = DB.getColumnBreakdown(table, column.getColumnName());
			System.out.println("");
			System.out.println(column.getColumnName());
			System.out.println("=================");
			System.out.println("Null Values: " + breakdown.getNumNullValues());
			if (breakdown.getNumNotNullValues() > 0) {
				System.out.println("Non-null Values: " + breakdown.getNumNotNullValues());
				System.out.println("Distinct Non-null Values: " + breakdown.getNumDistinctNonNullValues());
				System.out.println("Min Value: " + breakdown.getMinValue());
				System.out.println("Max Value: " + breakdown.getMaxValue());

				if (breakdown.getNumDistinctNonNullValues() != breakdown.getNumNotNullValues()) {
					System.out.println("Most Frequent Values:");
					Map<Object, Integer> mostFrequentValues = breakdown.getMostFrequentValues();
					for (Object value : mostFrequentValues.keySet()) {
						System.out.println(" * " + value + " : " + mostFrequentValues.get(value));
					}
				}
			}
		}
	}

	@Ignore
	@Test
	public void shouldGetAllUsers() throws Exception {
		Map<Integer, User> users = UserQuery.getUsers();
		for (User user : users.values()) {
			System.out.println(ExportUtil.toJson(user));
		}
	}

	@Test
	public void shouldGetAllPatients() throws Exception {
		Map<Integer, Patient> patientMap = PatientQuery.getPatients();
		List<Patient> patients = new ArrayList<Patient>(patientMap.values());
		System.out.println("Found " + patients.size() + " patients.  Here are a random 10");
		for (int i=0; i<10; i++) {
			int index = (int)(Math.random() * patients.size());
			Patient p = patients.get(index);
			System.out.println(ExportUtil.toJson(p));
		}
	}

	@Test
	public void shouldGetAllAddresses() throws Exception {
		ListMap<Integer, Address> addressesForPatients = PatientQuery.getAddresses();
		System.out.println("Found " + addressesForPatients.size() + " patients with addresses.  Here are the first 20");
		int i=0;
		for (Map.Entry<Integer, List<Address>> entry : addressesForPatients.entrySet()) {
			System.out.println(entry.getKey() + ": " + ExportUtil.toJson(entry.getValue()));
			if (i++==20) { break; }
		}
	}

	@Test
	public void shouldGetAllPamEnrollments() throws Exception {
		Map<Integer, PamEnrollment> enrollmentsForPatients = PatientQuery.getPamEnrollments();
		for (Map.Entry<Integer, PamEnrollment> entry : enrollmentsForPatients.entrySet()) {
			System.out.println(entry.getKey() + ": " + ExportUtil.toJson(entry.getValue()));
		}
	}
}
