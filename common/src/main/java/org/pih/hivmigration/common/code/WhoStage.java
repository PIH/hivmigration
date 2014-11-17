package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_symptoms.duration_unit
 */
public enum WhoStage implements CodedValue {

	ONE("1"),
	TWO("2"),
	THREE("3"),
	FOUR("4"),
	UNKNOWN("?");

	private String value;

	WhoStage(String value) {
		this.value = value;
	}

	@Override
	public List<String> getValues() {
		return Arrays.asList(value);
	}
}
