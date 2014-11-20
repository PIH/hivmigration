package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to cervical cancer screening encounters
 */
public enum CytologyPlan implements CodedValue {

	COLPOSCOPY,
	EVALUATION_WITH_GYN;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
