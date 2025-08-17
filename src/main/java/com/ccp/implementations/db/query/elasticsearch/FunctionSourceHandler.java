package com.ccp.implementations.db.query.elasticsearch;

import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
class FunctionSourceHandler  implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{
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
