package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to test_type in the hiv_lab_results table
 */
public enum LabTestTypeOrBrand implements CodedValue {

	EXAVIR;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
