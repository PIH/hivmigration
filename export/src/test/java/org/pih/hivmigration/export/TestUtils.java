package org.pih.hivmigration.export;

import junit.framework.Assert;
import org.apache.commons.beanutils.PropertyUtils;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.common.util.Util;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class TestUtils {

	public static DatabaseCredentials getDatabaseCredentials() {
		String homeDirectory = System.getProperty("user.home");
		Properties p = Util.loadPropertiesFromFile(new File(homeDirectory, "hivmigration.properties"));
		return new DatabaseCredentials(p.getProperty("connection.url"), p.getProperty("connection.username"), p.getProperty("connection.password"));
	}

	public static void assertCollectionSizeMatchesNumber(Collection toCheck, int expected) {
		Object firstInCollection = toCheck.iterator().next();
		int numInCollection = toCheck.size();
		if (firstInCollection instanceof Collection) {
			numInCollection = 0;
			for (Object o : toCheck) {
				numInCollection += ((Collection) o).size();
			}
		}
		Assert.assertEquals(expected, numInCollection);
	}

	public static int getNonNullPropertiesFoundInCollection(Collection<? extends Collection> toCheck, String propertyName) {
		int numFound = 0;
		try {
			for (Collection c : toCheck) {
				for (Object o : c) {
					if (PropertyUtils.getProperty(o, propertyName) != null) {
						numFound++;
					}
				}
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Error trying to get non null properties found in collection", e);
		}
		return numFound;
	}

	public static void assertCollectionSizeMatchesBaseTableSize(Collection toCheck, String table) {
		int numInTable = DB.uniqueResult("select count(*) from " + table, Integer.class);
		assertCollectionSizeMatchesNumber(toCheck, numInTable);
	}

	public static void assertCollectionSizeMatchesQuerySize(Collection toCheck, String query) {
		int numInQuery = DB.uniqueResult(query, Integer.class);
		assertCollectionSizeMatchesNumber(toCheck, numInQuery);
	}

	public static <T> void assertAllPropertiesArePopulated(Collection<T> toCheck) {
		Object representativeObject = toCheck.iterator().next();
		Set<String> writeableProperties = ExportUtil.getWriteableProperties(representativeObject);
		ListMap<String, Object> nestedPropertyValues = new ListMap<String, Object>();
		try {
			for (T object : toCheck) {
				for (Iterator<String> i = writeableProperties.iterator(); i.hasNext(); ) {
					String property = i.next();
					Object value = PropertyUtils.getNestedProperty(object, property);
					if (value instanceof Collection) {
						Collection c = (Collection) value;
						if (!c.isEmpty()) {
							nestedPropertyValues.putAll(property, c);
						}
					}
					else if (Util.notEmpty(value)) {
						i.remove();
					}
				}
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Error getting property values from object", e);
		}

		for (String s : nestedPropertyValues.keySet()) {
			writeableProperties.remove(s);
			assertAllPropertiesArePopulated(nestedPropertyValues.get(s));
		}

		Assert.assertEquals("Properties with no data on " + representativeObject.getClass().getSimpleName() + ": " + writeableProperties, 0, writeableProperties.size());
	}

	public static void assertEncounterDataOnlyIn(String type, String...tableNames) {
		List<Map<String, Object>> foreignKeys = DB.getForeignKeysToTable("hiv_encounters", "encounter_id");

		Set<String> tablesExpected = new HashSet<String>();
		for (String table : tableNames) {
			tablesExpected.add(table.toUpperCase());
		}

		Set<String> tablesFound = new HashSet<String>();
		for (Map<String, Object> m : foreignKeys) {
			String tableName = (String) m.get("tableName");
			String columnName = (String) m.get("columnName");

			StringBuilder query = new StringBuilder();
			query.append("select count(*) ");
			query.append("from hiv_encounters e, ").append(tableName).append(" t ");
			query.append("where e.encounter_id = t.").append(columnName);

			int numWithType = DB.uniqueResult(query.toString() + " and e.type = ?", Integer.class, type);
			if (numWithType > 0) {
				tablesFound.add(tableName.toUpperCase());
			}
		}

		Set<String> expectedButNotFound = new HashSet<String>(tablesExpected);
		expectedButNotFound.removeAll(tablesFound);

		Assert.assertTrue("Expected to find " + type + " encounters in " + expectedButNotFound + " but did not", expectedButNotFound.isEmpty());

		Set<String> foundButNotExpected = new HashSet<String>(tablesFound);
		foundButNotExpected.removeAll(tablesExpected);

		Assert.assertTrue("Expected to not find " + type + " encounters in " + foundButNotExpected + " but did", foundButNotExpected.isEmpty());
	}
}
