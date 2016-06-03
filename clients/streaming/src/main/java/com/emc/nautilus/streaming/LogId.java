package com.emc.nautilus.streaming;

import lombok.Data;
import lombok.NonNull;

@Data
public class LogId {
	private final String scope;
	@NonNull
	private final String name;
	private final int number;
	private final int previous;

	public LogId(String scope, String name, int number, int previous) {
		super();
		this.scope = scope;
		if (name == null) {
			throw new NullPointerException();
		}
		if (!name.matches("^\\w+\\z")) {
			throw new IllegalArgumentException("Name must be [a-zA-Z0-9]*");
		}
		this.name = name;
		this.number = number;
		this.previous = previous;
	}

	public String getQualifiedName() {
		StringBuffer sb = new StringBuffer();
		if (scope != null) {
			sb.append(scope);
			sb.append('/');
		}
		sb.append(name);
		sb.append('/');
		sb.append(number);
		return sb.toString();
	}

	/**
	 * @return True if this log is a replacement or partial replacement for the
	 *         one passed.
	 */
	public boolean succeeds(LogId other) {
		return ((scope == null) ? other.scope == null : scope.equals(other.scope)) && name.equals(other.name)
				&& previous == other.number;
	}
}
