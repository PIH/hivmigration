package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum TreatmentStatus implements CodedValue {

	ACTIVE_ARVS,
	ACTIVE_NO_ARVS,
	ACTIVE_NO_ARVS_AWAITING_EVAL,
	ACTIVE_NO_ARVS_NOT_YET_NEEDED,
	ACTIVE_NO_ARVS_PATIENT_REFUSED,
	ACTIVE_NO_ARVS_POSTPONED,
	ABANDONED,
	DIED,
	TRANSFERRED_OUT,
	TREATMENT_REFUSED,
	TREATMENT_STOPPED_OTHER,
	TREATMENT_STOPPED_SIDE_EFFECTS;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
