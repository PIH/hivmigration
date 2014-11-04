package org.pih.hivmigration.export;

import junit.framework.Assert;
import org.apache.commons.beanutils.PropertyUtils;
import org.pih.hivmigration.common.util.ListMap;
import org.pih.hivmigration.common.util.Util;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class TestUtils {

	public static DatabaseCredentials getDatabaseCredentials() {
		return new DatabaseCredentials("jdbc:oracle:thin:@localhost:1521:XE", "hiv", "hiv");
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

	public static void assertCollectionSizeMatchesBaseTableSize(Collection toCheck, String table) {
		int numInTable = DB.uniqueResult("select count(*) from " + table, BigDecimal.class).intValue();
		assertCollectionSizeMatchesNumber(toCheck, numInTable);
	}

	public static void assertCollectionSizeMatchesQuerySize(Collection toCheck, String query) {
		int numInQuery = DB.uniqueResult(query, BigDecimal.class).intValue();
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
}
