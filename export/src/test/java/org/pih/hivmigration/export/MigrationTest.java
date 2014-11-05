package org.pih.hivmigration.export;

import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pih.hivmigration.common.util.Util;
import org.pih.hivmigration.export.query.PatientQuery;

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
		String table = "";
		if (Util.notEmpty(table)) {
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
	}

	@Test
	public void shouldReturnColumnsWithData() throws Exception {
		String table = "";
		if (Util.notEmpty(table)) {
			System.out.println("*********************");
			System.out.println(table);
			System.out.println("*********************");
			for (TableColumn column : DB.getAllColumns(table)) {
				TableColumnBreakdown breakdown = DB.getColumnBreakdown(table, column.getColumnName());
				if (breakdown.getNumNotNullValues() > 0) {
					System.out.println(column.getColumnName().toLowerCase());
				}
			}
		}
	}

	@Test
	public void shouldReturnEncounterTypeBreakdown() throws Exception {
		String table = "";
		if (Util.notEmpty(table)) {
			StringBuilder query = new StringBuilder();
			query.append("select e.type, count(*) as num ");
			query.append("from " + table + " t, hiv_encounters e ");
			query.append("where t.encounter_id = e.encounter_id(+) ");
			query.append("group by e.type ");
			query.append("order by num desc");
			List<Map<String, Object>> results = DB.executeQuery(query.toString(), new MapListHandler());
			for (Map<String, Object> row : results) {
				System.out.println(row.get("TYPE") + ": " + row.get("NUM"));
			}
		}
	}

	@Test
	@Ignore
	public void shouldReturnObjectsAsJson() throws Exception {
		List l = new ArrayList(PatientQuery.getHivStatusData().values());
		System.out.println("Found: " + l.size() + " objects");
		for (int i=0; i<10; i++) {
			int index = (int)(Math.random() * l.size());
			System.out.println(index + ": " + ExportUtil.toJson(l.get(index)));
		}
	}
}
