package org.pih.hivmigration.export;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pih.hivmigration.common.util.Util;

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
}
