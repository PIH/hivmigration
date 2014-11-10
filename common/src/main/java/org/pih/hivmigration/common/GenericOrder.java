package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.OtherOrderable;

public class GenericOrder {

	private OtherOrderable ordered;
	private String comments;

	public GenericOrder() {}

	public OtherOrderable getOrdered() {
		return ordered;
	}

	public void setOrdered(OtherOrderable ordered) {
		this.ordered = ordered;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}
}
