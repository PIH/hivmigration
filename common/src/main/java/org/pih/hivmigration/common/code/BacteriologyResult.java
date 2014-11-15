package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum BacteriologyResult implements CodedValue {

	NEGATIVE("negative"),
	POSITIVE("positive"),
	ONE_PLUS("+"),
	TWO_PLUS("++"),
	THREE_PLUS("+++"),
	POOR_SAMPLE("poor sample"),
	INSUFFICIENT_SAMPLE("echantillon insuffisant"),
	NOT_DONE("not_done");

	private String shortValue;

	BacteriologyResult(String shortValue) {
		this.shortValue = shortValue;
	}

	@Override
	public List<String> getValues() {
		return Arrays.asList(shortValue);
	}
}
