package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.BodySystem;
import org.pih.hivmigration.common.code.Condition;

public class SystemStatus {

	private BodySystem system;
	private Condition condition;

	public SystemStatus() {}

	public BodySystem getSystem() {
		return system;
	}

	public void setSystem(BodySystem system) {
		this.system = system;
	}

	public Condition getCondition() {
		return condition;
	}

	public void setCondition(Condition condition) {
		this.condition = condition;
	}
}
