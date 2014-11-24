package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This codes out whether a patient is indicated as HIV or HIV-TB coinfected
 */
public enum TbTreatmentChangeReason implements CodedValue {

	CONTINUATION_PHASE,
	CURED,
	DOSE_CHANGE,
	EXTENDED_TREATMENT,
	FINISHED_TREATMENT,
	INEFFECTIVE,
	OTHER,
	SIDE_EFFECT,
	STOCK_OUT;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
