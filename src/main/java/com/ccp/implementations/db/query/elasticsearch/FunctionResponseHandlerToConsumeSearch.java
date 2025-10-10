package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.especifications.mensageria.receiver.CcpBusiness;

class FunctionResponseHandlerToConsumeSearch implements CcpBusiness{
	enum JsonFieldNames implements CcpJsonFieldName{
		hits, _scroll_id
	}
	private FunctionSourceHandler handler = new FunctionSourceHandler();
	
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		List<CcpJsonRepresentation> hits = json.getInnerJson(JsonFieldNames.hits).getAsJsonList(JsonFieldNames.hits);
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		String _scroll_id = json.getAsString(JsonFieldNames._scroll_id);
		return CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.hits, collect).put(JsonFieldNames._scroll_id, _scroll_id);
	}

}
