package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 */
public enum DoubleEntryStatus implements CodedValue {

	PRIMARY,
	SECONDARY;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
