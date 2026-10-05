package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.business.CcpBusiness;

import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * Helper {@code CcpBusiness} that extracts the {@code _source} field from an Elasticsearch hit
 * and adds the {@code id} and {@code entity} fields back to the resulting JSON.
 */
class FunctionSourceHandler implements CcpBusiness{
	/** Fields added to the source. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The document id. */
		id,
		/** The index name. */
		entity
	}


	/**
	 * Returns the {@code _source} of the hit plus {@code id} and {@code entity}.
	 * @param hit a search hit
	 * @return the document
	 */
	public CcpJsonRepresentation apply(CcpJsonRepresentation hit) {
		CcpJsonRepresentation source = hit.getInnerJson(CcpJsonCommonsFields._source);
		String entity = hit.getAsString(CcpJsonCommonsFields._index);
		String id = hit.getAsString(CcpJsonCommonsFields._id);
		CcpJsonRepresentation sourceWithId = source.put(JsonFieldNames.id, id);
		CcpJsonRepresentation sourceWithIdAndEntity = sourceWithId.put(JsonFieldNames.entity, entity);
		return sourceWithIdAndEntity;
	}
	
}
