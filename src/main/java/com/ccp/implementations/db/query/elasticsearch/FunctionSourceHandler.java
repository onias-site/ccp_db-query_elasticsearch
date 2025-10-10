package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.especifications.mensageria.receiver.CcpBusiness;

class FunctionSourceHandler implements CcpBusiness{
	enum JsonFieldNames implements CcpJsonFieldName{
		_source, _index, _id, id, entity
	}

	
	public CcpJsonRepresentation apply(CcpJsonRepresentation x) {
		CcpJsonRepresentation internalMap = x.getInnerJson(JsonFieldNames._source);
		String entity = x.getAsString(JsonFieldNames._index);
		String id = x.getAsString(JsonFieldNames._id);
		CcpJsonRepresentation put = internalMap.put(JsonFieldNames.id, id).put(JsonFieldNames.entity, entity);
		return put;
	}
	
}
