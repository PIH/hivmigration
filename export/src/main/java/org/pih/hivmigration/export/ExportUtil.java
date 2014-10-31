package org.pih.hivmigration.export;

import org.codehaus.jackson.map.ObjectMapper;
import org.pih.hivmigration.common.util.Util;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

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
