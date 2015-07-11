package edu.duke.dbmsplus.datahooks.yarnrecord;

import java.util.Map;

public class Event {
	private String eventtype;
	private Map<String, String> eventinfo;
	private long timestamp;

	public Event() {

	}

	public String getEventtype() {
		return eventtype;
	}

	public void setEventtype(String eventtype) {
		this.eventtype = eventtype;
	}

	public Map<String, String> getEventinfo() {
		return eventinfo;
	}

	public void setEventinfo(Map<String, String> eventinfo) {
		this.eventinfo = eventinfo;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
