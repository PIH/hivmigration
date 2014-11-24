package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum TreatmentStoppedReason implements CodedValue {

	CD4_AUGMENTED,
	SIDE_EFFECTS,
	TREATMENT_COMPLETE;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
