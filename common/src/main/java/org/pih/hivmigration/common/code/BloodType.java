package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum BloodType implements CodedValue {

	AB_NEGATIVE,
	AB_POSITIVE,
	B_NEGATIVE,
	B_POSITIVE,
	O_NEGATIVE,
	O_POSITIVE,
	A_NEGATIVE,
	A_POSITIVE;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().replace("_POSITIVE", "+").replace("_NEGATIVE", "-"));
	}
}
