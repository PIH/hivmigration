package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the various lab test results
 */
public enum DstResult implements CodedValue {

	PAN_SENSITIVE("pan-sensitive:"),
	RESISTANT_TO_HREZ("resistant:HERZ", "resistant:RHEZ");

	private List<String> matchingValues;

	DstResult(String...values) {
		matchingValues = Arrays.asList(values);
	}

	@Override
	public List<String> getValues() {
		return matchingValues;
	}
}
