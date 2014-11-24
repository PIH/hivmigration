package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * Duration of diarrhea symptom
 */
public enum DiarrheaDuration implements CodedValue {

	OVER_1_MONTH,
	UNDER_1_MONTH;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
