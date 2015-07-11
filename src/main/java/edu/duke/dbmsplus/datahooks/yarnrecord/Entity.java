package edu.duke.dbmsplus.datahooks.yarnrecord;

import java.util.List;
import java.util.Map;

/**
 * http(s)://<timeline server http(s)
 * address:port>/ws/v1/timeline/{entityType}/{entityId}
 * 
 * @author hadoop
 *
 */
public class Entity {
	private String entity;
	private String entitytype;
	/**
	 * not sure argument types of Map
	 */
	private Map<String, Entity> relatedentities;
	private List<Event> events;
	private Map<String, String> primaryfilters;
	/**
	 * not sure argument types of Map
	 */
	private Map<String, String> otherinfo;
	private long starttime;

	public Entity() {

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

	public Map<String, Entity> getRelatedentities() {
		return relatedentities;
	}

	public void setRelatedentities(Map<String, Entity> relatedentities) {
		this.relatedentities = relatedentities;
	}

	public List<Event> getEvents() {
		return events;
	}

	public void setEvents(List<Event> events) {
		this.events = events;
	}

	public Map<String, String> getPrimaryfilters() {
		return primaryfilters;
	}

	public void setPrimaryfilters(Map<String, String> primaryfilters) {
		this.primaryfilters = primaryfilters;
	}

	public Map<String, String> getOtherinfo() {
		return otherinfo;
	}

	public void setOtherinfo(Map<String, String> otherinfo) {
		this.otherinfo = otherinfo;
	}

	public long getStarttime() {
		return starttime;
	}

	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}

}
