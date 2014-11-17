package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum PartnerReferralStatus implements CodedValue {

	REFERRED("t"),
	NOT_REFERRED("f"),
	NO_PARTNER_IDENTIFIED("9");

	private String value;

	PartnerReferralStatus(String value) {
		this.value = value;
	}

	@Override
	public List<String> getValues() {
		return Arrays.asList(value);
	}
}
