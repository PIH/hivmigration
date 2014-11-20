/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.pih.hivmigration.common.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.pih.hivmigration.common.ObsName;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Util {

	public static String toString(Collection<?> c) {
		return toString(c, ",");
	}

	public static String toString(Collection<?> c, String separator) {
		StringBuilder ret = new StringBuilder();
		if (c != null) {
			for (Object o : c) {
				ret.append(ret.length() == 0 ? "" : separator).append(o);
			}
		}
		return ret.toString();
	}

	public static String toString(Object[] c) {
		if (c != null) {
			return toString(Arrays.asList(c));
		}
		return "";
	}

	public static String toString(Object[] c, String separator) {
		if (c != null) {
			return toString(Arrays.asList(c), separator);
		}
		return "";
	}

	public static List<String> toList(String s, String separator) {
		List<String> ret = new ArrayList<String>();
		for (String element : s.split(separator)) {
			ret.add(element);
		}
		return ret;
	}

	public static <K, V> Map<K, V> toMap(Object... keysAndValues) {
		Map<K, V> m = new LinkedHashMap<K, V>();
		for (int i=0; i<keysAndValues.length; i+=2) {
			m.put((K)keysAndValues[i], (V)keysAndValues[i+1]);
		}
		return m;
	}

	public static boolean isEmpty(Object o) {
		return o == null || o.equals("");
	}

	public static boolean notEmpty(Object o) {
		return !isEmpty(o);
	}

	public static <T> T firstNotNull(T... values) {
		for (T val : values) {
			if (notEmpty(val)) {
				return val;
			}
		}
		return null;
	}

	public static <T> T nvl(T o, T valueIfNull) {
		if (isEmpty(o)) {
			return valueIfNull;
		}
		return o;
	}

	public static String nvlStr(Object o, String valueIfNull) {
		if (isEmpty(o)) {
			return valueIfNull;
		}
		return o.toString();
	}

	public static boolean areEqualStr(Object o1, Object o2) {
		if (o1 == o2) {
			return true;
		}
		return nvlStr(o1, "").equals(nvlStr(o2, ""));
	}

	public static String formatDate(Date d, String format) {
		SimpleDateFormat df = new SimpleDateFormat(format);
		return df.format(d);
	}

	/**
	 * @param toParse the string to parse into a Map<String, String>
	 * @param keyValueSeparator the string that separates the entries for the Map, if null defaults to "="
	 * @param entrySeparator the string that separates each key/value pair in the Map, if null defaults to ","
	 * @return
	 */
	public static Map<String, String> toMap(String toParse, String keyValueSeparator, String entrySeparator) {
		Map<String, String> ret = new LinkedHashMap<String, String>();
		if (notEmpty(toParse)) {
			for (String entry : StringUtils.splitByWholeSeparator(toParse, nvlStr(entrySeparator, ","))) {
				String[] keyValue = StringUtils.splitByWholeSeparator(entry, nvlStr(keyValueSeparator, "="), 2);
				ret.put(keyValue[0], keyValue[1]);
			}
		}
		return ret;
	}

	/**
	 * @param toParse the string to parse into a Map<String, String>. Expected format is key1=value1,key2=value2...
	 * @return
	 */
	public static Map<String, String> toMap(String toParse) {
		return toMap(toParse, "=", ",");
	}

	public static String loadFromFile(String path) {
		String ret = null;
		try {
			ret = FileUtils.readFileToString(new File(path), "UTF-8");
		}
		catch (Exception e) {}
		return ret;
	}

	public static List<String> loadFromClasspath(String path) {
		List<String> ret = new ArrayList<String>();
		InputStream is = null;
		try {
			is = Util.class.getClassLoader().getResourceAsStream(path);
			ret = IOUtils.readLines(is, "UTF-8");
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Unable to load from classpath: " + path, e);
		}
		finally {
			IOUtils.closeQuietly(is);
		}
		return ret;
	}

	public static String encodeString(String strToEncode) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-512");
			byte[] input = strToEncode.getBytes("UTF-8");
			return hexString(md.digest(input));
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to encode string " + strToEncode, e);
		}
	}

	public static String hexString(byte[] block) {
		StringBuffer buf = new StringBuffer();
		char[] hexChars = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
		int len = block.length;
		int high = 0;
		int low = 0;
		for (int i = 0; i < len; i++) {
			high = ((block[i] & 0xf0) >> 4);
			low = (block[i] & 0x0f);
			buf.append(hexChars[high]);
			buf.append(hexChars[low]);
		}
		return buf.toString();
	}

	/**
	 * @return a Properties object based on a properties file at the specified location
	 */
	public static Properties loadPropertiesFromFile(File file) {
		Properties ret = new Properties();
		InputStream is = null;
		try {
			is = FileUtils.openInputStream(file);
			ret.load(is);
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to load properties from file at " + file.getAbsolutePath(), e);
		}
		finally {
			IOUtils.closeQuietly(is);
		}
		return ret;
	}

	public static boolean isInListIgnoreCaseAndWhitespace(String stringToCheck, List<String> listToCheck) {
		stringToCheck = stringToCheck.trim();
		for (String listItem : listToCheck) {
			if (listItem.trim().equalsIgnoreCase(stringToCheck)) {
				return true;
			}
		}
		return false;
	}

	public static Double parseDoubleIfPossible(String stringToCheck) {
		try {
			Double d = Double.valueOf(stringToCheck);
			return d;
		}
		catch (Exception e) {
			return null;
		}
	}

	public static boolean isDouble(String stringToCheck) {
		return parseDoubleIfPossible(stringToCheck) != null;
	}

	public static Map<String, String> getPropertyToObsNameMap(Class<?> classToCheck) {
		Map<String, String> ret = new LinkedHashMap<String, String>();

		// If this class extends another class, then inspect all inherited field values as well
		Class superclass = classToCheck.getSuperclass();
		if (superclass != null) {
			ret.putAll(getPropertyToObsNameMap(superclass));
		}

		// Iterate across all of the declared fields in the passed class
		for (Field f : classToCheck.getDeclaredFields()) {
			ObsName ann = f.getAnnotation(ObsName.class);
			if (ann != null) {
				ret.put(f.getName(), Util.nvlStr(ann.value(), f.getName()));
			}
		}
		return ret;
	}

	public static Class<?> getFieldType(Class<?> classToCheck, String propertyName) {
		try {
			Field f = classToCheck.getDeclaredField(propertyName);
			return f.getType();
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Unable to find property named " + propertyName + " on class " + classToCheck);
		}
	}
}

