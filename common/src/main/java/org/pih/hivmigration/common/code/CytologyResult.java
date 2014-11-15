package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to hiv_exam_lab_results cytology result
 */
public enum CytologyResult implements CodedValue {

	ASCUS,
	HSIL,
	LSIL;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
