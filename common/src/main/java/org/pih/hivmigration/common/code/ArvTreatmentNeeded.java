package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum ArvTreatmentNeeded implements CodedValue {

	NO,
	IN_PROGRESS,
	YES,
	YES_AFTER_ACCOMP,
	YES_CONTINUE,
	YES_DEFERRED,
	YES_IMMEDIATELY,
	YES_PTME,
	YES_REFUSED;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
