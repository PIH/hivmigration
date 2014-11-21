package org.pih.hivmigration.export;

import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.pih.hivmigration.common.util.Util;
import org.pih.hivmigration.export.query.PatientQuery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Ignore
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
	public void shouldReturnEnumValuesForColumn() {
		String query = "";
		query = "select distinct lab_test from HIV_EXAM_LAB_RESULTS where result is not null order by lab_test";
		//String query = "select symptom, count(*) from hiv_exam_symptoms where lower(trim(symptom)) = symptom and replace(symptom, ' ', '') = symptom group by symptom having count(*) > 100 order by symptom";
		if (Util.notEmpty(query)) {
			List<String> ret = DB.listResult(query, String.class);
			for (String s : ret) {
				System.out.println(s);
			}
			System.out.println("");
			System.out.println("As Enum Code");
			System.out.println("");
			for (String s : ret) {
				System.out.println("\t"+s.toUpperCase()+",");
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
	public void shouldReturnEncounterColumnsForType() throws Exception {
		List<String> types = DB.listResult("select distinct type from hiv_encounters", String.class);
		List<TableColumn> columns = DB.getAllColumns("HIV_ENCOUNTERS");
		for (TableColumn column : columns) {
			List<String> found = new ArrayList<String>();
			for (String type : types) {
				Integer num = DB.uniqueResult("select count(*) from hiv_encounters where type = ? and " + column.getColumnName() + " is not null", Integer.class, type);
				if (num > 0) {
					found.add(type);
				}
			}
			System.out.println(column.getColumnName() + ": " + found);
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
	public void shouldTestLabResults() {
		List<String> labTests = DB.listResult("select distinct lab_test from hiv_exam_lab_results order by lab_test", String.class);
		for (String labTest : labTests) {
			List<String> distinctResults = DB.listResult("select distinct result from hiv_exam_lab_results where result is not null and lab_test = ?", String.class, labTest);
			if (distinctResults.size() < 20) {
				System.out.println(labTest + ": " + distinctResults);
			}
			else {
				int numNumeric = 0;
				for (String result : distinctResults) {
					try {
						Double.parseDouble(result);
						numNumeric++;
					}
					catch (Exception e) {}
				}
				System.out.println(labTest + ": " + numNumeric + " / " + distinctResults.size() + " results are numeric");
			}
		}
	}

	@Test
	public void shouldReturnAllTablesWithDataForEncounterType() throws Exception {
		boolean enabled = true;
		if (enabled) {
			Map<String, Map<String, Integer>> typeToTableMap = new LinkedHashMap<String, Map<String, Integer>>();
			Map<String, Set<String>> tableToTypeMap = new LinkedHashMap<String, Set<String>>();

			List<String> encounterTypes = DB.listResult("select distinct type from hiv_encounters order by type", String.class);
			for (String encounterType : encounterTypes) {
				if ("accompagnateur".equals(encounterType)) {
					Map<String, Integer> tableCounts = new LinkedHashMap<String, Integer>();
					typeToTableMap.put(encounterType, tableCounts);
					List<Map<String, Object>> rows = DB.getForeignKeysToTable("hiv_encounters", "encounter_id" );
					for (Map<String, Object> row : rows) {
						String tableName = (String) row.get("tableName" );
						String columnName = (String) row.get("columnName" );

						StringBuilder query = new StringBuilder();
						query.append("select 	count(*) " );
						query.append("from 		" ).append(tableName).append(" t, hiv_encounters e " );
						query.append("where		t." ).append(columnName).append(" = e.encounter_id " );
						query.append("and		e.type = ? " );

						Integer numFound = DB.uniqueResult(query.toString(), Integer.class, encounterType);
						if (numFound > 0) {
							for (TableColumn column : DB.getAllColumns(tableName)) {
								StringBuilder q = new StringBuilder();
								q.append("select 	count(*) " );
								q.append("from 		" ).append(tableName).append(" t, hiv_encounters e " );
								q.append("where		t." ).append(columnName).append(" = e.encounter_id " );
								q.append("and		e.type = ? " );
								q.append("and		t." ).append(column.getColumnName().toLowerCase()).append(" is not null" );
								Integer numColValues = DB.uniqueResult(q.toString(), Integer.class, encounterType);

								if (numColValues > 0) {
									Set<String> typeSet = tableToTypeMap.get(tableName);
									if (typeSet == null) {
										typeSet = new LinkedHashSet<String>();
										tableToTypeMap.put(tableName, typeSet);
									}

									tableCounts.put(tableName + "." + column.getColumnName(), numColValues);
									typeSet.add(encounterType);
								}
							}
						}
					}
				}
			}

			System.out.println("Tables with data for each encounter type" );
			for (String encounterType : typeToTableMap.keySet()) {
				System.out.println("" );
				System.out.println(" ***** " + encounterType + " ***** " );
				Map<String, Integer> tableCounts = typeToTableMap.get(encounterType);
				for (String tabCol : tableCounts.keySet()) {
					System.out.println(tabCol + ": " + tableCounts.get(tabCol));
				}
				System.out.println("" );
			}
			System.out.println("" );

			System.out.println("Encounter types with data for each table" );
			for (String table : tableToTypeMap.keySet()) {
				System.out.println(table + ": " + tableToTypeMap.get(table));
			}

		}
	}

	@Test
	@Ignore
	public void shouldReturnObjectsAsJson() throws Exception {
		List l = new ArrayList(PatientQuery.getIntakeEncounters().values());
		System.out.println("Found: " + l.size() + " objects");
		for (int i=0; i<5; i++) {
			int index = (int)(Math.random() * l.size());
			System.out.println(index + ": " + ExportUtil.toJson(l.get(index)));
		}
	}
}
