package org.pih.hivmigration.common.code;

/**
 * This corresponds to the hiv_exam_system_status.condition values
 */
public enum Condition implements CodedValue {

	NORMAL,
	NONE,
	PALE,
	DISCHARGE,
	CERVICAL,
	CREPITANCE,
	ULCERS,
	INGINUAL,
	EDEMA,
	RALES,
	ICTERIC,
	AXILLARY,
	WHEEZE,
	ASCITES,
	SPLENOMEGALY,
	HEPATOMEGALY;

	public String getValue() {
		return name().toLowerCase();
	}
}
