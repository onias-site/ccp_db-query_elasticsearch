package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.business.CcpBusiness;

import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * {@code CcpBusiness} auxiliar que extrai o campo {@code _source} de um hit do Elasticsearch
 * e re-adiciona os campos {@code id} e {@code entity} ao JSON resultante.
 */
class FunctionSourceHandler implements CcpBusiness{
	enum JsonFieldNames implements CcpJsonFieldName{ id, entity
	}

	
	public CcpJsonRepresentation apply(CcpJsonRepresentation x) {
		CcpJsonRepresentation internalMap = x.getInnerJson(CcpJsonCommonsFields._source);
		String entity = x.getAsString(CcpJsonCommonsFields._index);
		String id = x.getAsString(CcpJsonCommonsFields._id);
		CcpJsonRepresentation put2 = internalMap.put(JsonFieldNames.id, id);
		CcpJsonRepresentation put = put2.put(JsonFieldNames.entity, entity);
		return put;
	}
	
}
