package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_symptoms.duration_unit
 */
public enum ActivityFrequency implements CodedValue {

	NO,
	SOMETIMES,
	EVERY_DAY,
	ALWAYS,
	NEVER;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
