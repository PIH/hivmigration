package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.OI;

public class OpportunisticInfection {

	private OI oi;
	private String comments;

	public OpportunisticInfection() {}

	public OI getOi() {
		return oi;
	}

	public void setOi(OI oi) {
		this.oi = oi;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}
}
