package edu.duke.dbmsplus.datahooks.yarnrecord;

/**
 * http(s)://<timeline server http(s)
 * address:port>/ws/v1/timeline/{entityType}/events
 * 
 * @author Xiaodan
 *
 */
public class EventList {
	private String entity;
	private String entitytype;
	private Event[] events;

	public EventList() {

	}

	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public String getEntitytype() {
		return entitytype;
	}

	public void setEntitytype(String entitytype) {
		this.entitytype = entitytype;
	}

	public Event[] getEvents() {
		return events;
	}

	public void setEvents(Event[] events) {
		this.events = events;
	}

}
