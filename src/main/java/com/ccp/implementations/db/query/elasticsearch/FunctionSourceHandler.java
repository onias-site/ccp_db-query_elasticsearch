package com.ccp.implementations.db.query.elasticsearch;

import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
enum FunctionSourceHandlerConstants  implements CcpJsonFieldName{
	_source, _index, _id, id, entity
	
}
class FunctionSourceHandler  implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{

	
	public CcpJsonRepresentation apply(CcpJsonRepresentation x) {
		CcpJsonRepresentation internalMap = x.getInnerJson(FunctionSourceHandlerConstants._source);
		String entity = x.getAsString(FunctionSourceHandlerConstants._index);
		String id = x.getAsString(FunctionSourceHandlerConstants._id);
		CcpJsonRepresentation put = internalMap.put(FunctionSourceHandlerConstants.id, id).put(FunctionSourceHandlerConstants.entity, entity);
		return put;
	}
	
}
