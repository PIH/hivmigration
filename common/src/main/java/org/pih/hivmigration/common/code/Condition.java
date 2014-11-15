package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

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

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
