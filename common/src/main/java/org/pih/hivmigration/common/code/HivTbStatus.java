package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This codes out whether a patient is indicated as HIV or HIV-TB coinfected
 */
public enum HivTbStatus implements CodedValue {

	HIV("HIV","hiv_only"),
	HIV_TB("hiv_tb","hiv_tb_coinfected");

	private List<String> values;

	HivTbStatus(String... values) {
		this.values = Arrays.asList(values);
	}

	@Override
	public List<String> getValues() {
		return values;
	}
}
