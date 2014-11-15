package org.pih.hivmigration.export;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pih.hivmigration.common.util.Util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Ignore
public class CleanupTest {

	@Before
	public void beforeTest() throws Exception {
		DB.openConnection(TestUtils.getDatabaseCredentials());
	}

	@After
	public void afterTest() throws Exception {
		DB.closeConnection();
	}

	@Test
	public void cleanupLabTestsAssociatedWithIncorrectEncounters() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("select e.patient_id, e.entry_date, e.encounter_date, e.entered_by, r.encounter_id, r.lab_test, r.result, r.test_date ");
		sb.append("from hiv_encounters e, hiv_exam_lab_results r where e.encounter_id = r.encounter_id ");
		sb.append("and e.type in ('regime', 'food_support', 'note', 'accompagnateur') order by e.encounter_id");
		List<Map<String, Object>> results = DB.tableResult(sb.toString());
		for (Map<String, Object> row : results) {
			StringBuilder insertQuery = new StringBuilder();
			Object newEncounterId = DB.uniqueResult("select hiv_encounter_id_seq.nextval from dual", BigDecimal.class);
			insertQuery.append("insert into hiv_encounters (encounter_id, patient_id, entry_date, encounter_date, entered_by, type) values (");
			insertQuery.append("?, ?, ?, ?, ?, 'lab_result')");

			System.out.println("Executing: " + insertQuery.toString());
			//DB.executeUpdate(insertQuery.toString(), newEncounterId, row.get("PATIENT_ID"), row.get("ENTRY_DATE"), row.get("ENCOUNTER_DATE"), row.get("ENTERED_BY"));

			StringBuilder updateQuery = new StringBuilder();
			updateQuery.append("update hiv_exam_lab_results set encounter_id = " + newEncounterId + " where encounter_id = " + row.get("ENCOUNTER_ID"));

			System.out.println("Executing: " + updateQuery.toString());
			//DB.executeUpdate(updateQuery.toString());
		}
	}

	@Test
	public void cleanupExamLabResults() throws Exception {

		String testName = "ppd";
		List<String> negatives = Arrays.asList("negatif`", "negatif", "neg", "NEG", "Negatif", "NEGATIF", "neagtif", "négatif", "Negative", "nÃ©gatif", "NGATIF", "neatif", "  negatif", "ngatif", "NÃ©gatif", "Neg", "nagatif");
		List<String> positives = Arrays.asList("positif", "POSITIF","pos", "Positif");
		List<String> units = Arrays.asList("mms/hres", "mm/h", "mmm", "MM", "mm", "mms", "mn", "m");

		if (Util.notEmpty(testName)) {
			String resultQuery = "select r.encounter_id, e.encounter_date, r.lab_test, r.result from hiv_exam_lab_results r, hiv_encounters e where r.encounter_id = e.encounter_id and r.result is not null and r.lab_test = ?";
			List<Map<String, Object>> results = DB.tableResult(resultQuery, testName);
			for (Map<String, Object> row : results) {
				String result = (String) row.get("result");
				if (result != null) {
					String[] resultComponents = result.split(":");
					List<String> newResults = new ArrayList<String>();
					for (String resultComponent : resultComponents) {

						String newResult = resultComponent.trim();

						for (String s : negatives) {
							if (newResult.equalsIgnoreCase(s)) {
								newResult = "negative";
							}
						}
						for (String s : positives) {
							if (newResult.equalsIgnoreCase(s)) {
								newResult = "positive";
							}
						}
						for (String s : units) {
							if (newResult.endsWith(s)) {
								newResult = newResult.substring(0, newResult.lastIndexOf(s));
								break;
							}
						}
						newResult = newResult.trim();
						if (newResult.equalsIgnoreCase("o")) {
							newResult = "0";
						}

						if (Util.notEmpty(newResult)) {
							newResults.add(newResult);
						}
					}

					String newResult = Util.toString(newResults, ":");

					if (!result.equalsIgnoreCase(newResult)) {
						System.out.println(row.get("ENCOUNTER_DATE") + ": Updating result from " + result + " -> " + newResult);
						//String updateQuery = "update hiv_exam_lab_results set result = ? where encounter_id = ? and lab_test = ?";
						//DB.executeUpdate(updateQuery, newResult, row.get("ENCOUNTER_ID"), testName);
					}
				}
			}
		}
	}
}
