package edu.duke.dbmsplus.datahooks.yarnrecord;
/**
 * Domain Object
 * GET /ws/v1/timeline/domain/{domainId}
 * @author Xiaodan
 *
 */
public class Domain {
	
	private String id;
	private String owner;
	private String readers;
	private String writers;
	private long createdtime;
	private long modifiedtime;
	
	public Domain() {
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getReaders() {
		return readers;
	}

	public void setReaders(String readers) {
		this.readers = readers;
	}

	public String getWriters() {
		return writers;
	}

	public void setWriters(String writers) {
		this.writers = writers;
	}

	public long getCreatedtime() {
		return createdtime;
	}

	public void setCreatedtime(long createdtime) {
		this.createdtime = createdtime;
	}

	public long getModifiedtime() {
		return modifiedtime;
	}

	public void setModifiedtime(long modifiedtime) {
		this.modifiedtime = modifiedtime;
	}
	
}
