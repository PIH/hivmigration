package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_symptoms.duration_unit
 */
public enum DurationUnit implements CodedValue {

	DAYS,
	MONTHS,
	WEEKS,
	YEARS;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
