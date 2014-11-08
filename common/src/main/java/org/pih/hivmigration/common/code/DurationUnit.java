package org.pih.hivmigration.common.code;

/**
 * This corresponds to the hiv_exam_symptoms.duration_unit
 */
public enum DurationUnit implements CodedValue {

	DAYS,
	MONTHS,
	WEEKS,
	YEARS;

	public String getValue() {
		return name().toLowerCase();
	}
}
