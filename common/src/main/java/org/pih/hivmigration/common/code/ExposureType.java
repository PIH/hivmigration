package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the reason for arvs as accident
 */
public enum ExposureType implements CodedValue {

	NO,
	ACCIDENT,
	BLOOD_EXPOSURE,
	PROPHYLAXIS,
	RAPE,
	SEXUAL_EXPOSURE;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
