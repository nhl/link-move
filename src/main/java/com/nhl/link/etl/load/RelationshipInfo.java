package com.nhl.link.etl.load;

import org.apache.cayenne.DataObject;

public class RelationshipInfo {
	private final String name;

	private final String keyAttribute;

	private final RelationshipType type;

	private final Class<? extends DataObject> objectType;

	private final String relationshipKeyAttribute;

	public RelationshipInfo(String name, String keyAttribute, RelationshipType type,
			Class<? extends DataObject> objectType) {
		this(name, keyAttribute, type, objectType, null);
	}

	public RelationshipInfo(String name, String keyAttribute, RelationshipType type,
			Class<? extends DataObject> objectType, String relationshipKeyAttribute) {
		this.name = name;
		this.keyAttribute = keyAttribute;
		this.type = type;
		this.objectType = objectType;
		this.relationshipKeyAttribute = relationshipKeyAttribute;
	}

	public String getName() {
		return name;
	}

	public String getKeyAttribute() {
		return keyAttribute;
	}

	public RelationshipType getType() {
		return type;
	}

	public Class<? extends DataObject> getObjectType() {
		return objectType;
	}

	public String getRelationshipKeyAttribute() {
		return relationshipKeyAttribute;
	}
}
