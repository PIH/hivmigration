package org.pih.hivmigration.export;

import org.apache.commons.beanutils.PropertyUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.pih.hivmigration.common.CodedOrNonCoded;
import org.pih.hivmigration.common.User;
import org.pih.hivmigration.common.code.CodedValue;
import org.pih.hivmigration.common.util.Util;
import org.pih.hivmigration.export.query.UserQuery;

import java.beans.PropertyDescriptor;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExportUtil {

	public static List<String> getTablesSpecifiedInResource(String resourceName) {
		List<String> ret = new ArrayList<String>();
		List<String> resourceLines = Util.loadFromClasspath(resourceName + ".txt");
		for (String line : resourceLines) {
			if (Util.notEmpty(line)) {
				line = line.trim();
				if (!line.startsWith("#")) {
					ret.add(line);
				}
			}
		}
		return ret;
	}

	public static <T> T getValueFromMap(Class<T> type, String name, Map<String, Object> values) {
		for (String key : values.keySet()) {
			if (key.toLowerCase().replace("_", "").equalsIgnoreCase(name)) {
				return convertValue(values.get(key), type);
			}
		}
		throw new IllegalArgumentException("Unable to find " + name + " in " + values);
	}

	public static <T> T convertValue(Object value, Class<T> type) {
		Object ret = value;
		if (value != null) {
			if (type == Integer.class) {
				ret = Integer.valueOf(value.toString());
			}
			else if (type == String.class) {
				ret = value.toString();
			}
			else if (type == Date.class) {
				ret = value;
			}
			else if (type == Boolean.class) {
				if ("t".equals(value) || "T".equals(value)) {
					ret = Boolean.TRUE;
				}
				else if ("f".equals(value) || "F".equals(value)) {
					ret = Boolean.FALSE;
				}
				else {
					throw new IllegalArgumentException("Not able to convert " + value + " to boolean");
				}
			}
			else if (type == User.class) {
				ret = UserQuery.getUser(value);
			}
			else if (CodedValue.class.isAssignableFrom(type) && type.isEnum()) {
				CodedValue cv = getCodedValue((Class<CodedValue>)type, value.toString());
				if (cv == null) {
					throw new IllegalArgumentException("Unable to convert " + value + " to an enum of type: " + type);
				}
				ret = cv;
			}
			else if (CodedOrNonCoded.class.isAssignableFrom(type)) {
				try {
					CodedOrNonCoded codedOrNonCoded = (CodedOrNonCoded) type.newInstance();
					CodedValue cv = getCodedValue(codedOrNonCoded.getCodedValueType(), value.toString());
					codedOrNonCoded.setCodedValue(cv);
					codedOrNonCoded.setNonCodedValue((cv == null ? value.toString() : null));
					ret = codedOrNonCoded;
				}
				catch (Exception e) {
					throw new IllegalArgumentException("Unable to convert " + value + " to " + type, e);
				}
			}
		}
		return (T)ret;
	}

	public static <T extends CodedValue> T getCodedValue(Class<T> type, String value) {
		for (Object o : type.getEnumConstants()) {
			CodedValue cv = (CodedValue)o;
			if (cv.getValue().equalsIgnoreCase(value.toString())) {
				return (T)cv;
			}
		}
		return null;
	}

	public static <T> T toObject(Class<T> type, Map<String, Object> values) {
		try {
			if (Map.class.isAssignableFrom(type)) {
				return (T)values;
			}
			T o = type.newInstance();
			for (PropertyDescriptor descriptor : PropertyUtils.getPropertyDescriptors(type)) {
				if (PropertyUtils.isWriteable(o, descriptor.getName())) {
					Object val = getValueFromMap(descriptor.getPropertyType(), descriptor.getName(), values);
					PropertyUtils.setProperty(o, descriptor.getName(), val);
				}
			}
			return o;
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Unable to populate " + type + " with " + values, e);
		}
	}

	public static Set<String> getWriteableProperties(Object o) {
		Set<String> ret = new HashSet<String>();
		for (PropertyDescriptor pd : PropertyUtils.getPropertyDescriptors(o.getClass())) {
			if (PropertyUtils.isWriteable(o, pd.getName())) {
				ret.add(pd.getName());
			}
		}
		return ret;
	}

	public static String toJson(Object o) {
		StringWriter writer = new StringWriter();
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(writer, o);
		}
		catch (Exception e) {
			throw new IllegalStateException("Unable to write object to json", e);
		}
		return writer.toString();
	}
}
