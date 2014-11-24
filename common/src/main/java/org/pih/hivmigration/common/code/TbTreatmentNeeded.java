package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum TbTreatmentNeeded implements CodedValue {

	NO,
	YES,
	EXAM_IN_PROGRESS,
	ALREADY_IN_TREATMENT,
	CONTINUE_NO_CHANGE,
	PRIOR_NOT_INDICATED,
	START_TX,
	CHANGE_TX,
	STOP_TX;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
