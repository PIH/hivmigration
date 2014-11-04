package org.pih.hivmigration.export;

import org.apache.commons.beanutils.PropertyUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.pih.hivmigration.common.User;
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
		}
		return (T)ret;
	}

	public static <T> T toObject(Class<T> type, Map<String, Object> values) {
		try {
			T o = type.newInstance();
			for (PropertyDescriptor descriptor : PropertyUtils.getPropertyDescriptors(type)) {
				if (PropertyUtils.isWriteable(o, descriptor.getName())) {
					PropertyUtils.setProperty(o, descriptor.getName(), getValueFromMap(descriptor.getPropertyType(), descriptor.getName(), values));
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
