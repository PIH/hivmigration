package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the various lab test results
 */
public enum SimpleLabResult implements CodedValue {

	POSITIVE("positive", "t"),
	NEGATIVE("negative", "f"),
	UNKNOWN("unknown", "undetermined", "?");

	private List<String> matchingValues;

	SimpleLabResult(String...values) {
		matchingValues = Arrays.asList(values);
	}

	@Override
	public List<String> getValues() {
		return matchingValues;
	}
}
