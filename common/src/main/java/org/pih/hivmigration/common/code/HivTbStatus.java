package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to cervical cancer screening encounters
 */
public enum HivTbStatus implements CodedValue {

	HIV,
	HIV_TB;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
