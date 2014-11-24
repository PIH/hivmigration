package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the severity of a rash etc
 */
public enum Severity implements CodedValue {

	LIGHT,
	MODERATE,
	SEVERE;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
