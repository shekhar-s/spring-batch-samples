package com.shekhar.spring.batch;

public class MyEntity {
	private long entityCode;
	private String entityName;
	private String entityDescription;

	public MyEntity(){}
	
	public MyEntity(long entityCode, String entityName, String entityDescription) {
		this.entityCode = entityCode;
		this.entityName = entityName;
		this.entityDescription = entityDescription;
	}

	public long getEntityCode() {
		return entityCode;
	}

	public void setEntityCode(long entityCode) {
		this.entityCode = entityCode;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public void setEntityDescription(String entityDescription) {
		this.entityDescription = entityDescription;
	}

	public String getEntityName() {
		return entityName;
	}

	public String getEntityDescription() {
		return entityDescription;
	}

}
