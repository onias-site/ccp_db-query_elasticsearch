package com.ccp.implementations.db.query.elasticsearch;

import java.util.function.Function;

import com.ccp.decorators.CcpJsonRepresentation;

class CcpSourceHandler  implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{

	
	public CcpJsonRepresentation apply(CcpJsonRepresentation x) {
		CcpJsonRepresentation internalMap = x.getInnerJson("_source");
		String entity = x.getAsString("_index");
		String id = x.getAsString("_id");
		CcpJsonRepresentation put = internalMap.put("id", id).put("entity", entity);
		return put;
	}
	
}
