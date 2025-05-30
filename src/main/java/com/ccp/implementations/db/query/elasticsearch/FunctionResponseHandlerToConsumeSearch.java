package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;

class FunctionResponseHandlerToConsumeSearch implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	private FunctionSourceHandler handler = new FunctionSourceHandler();
	
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		List<CcpJsonRepresentation> hits = json.getInnerJson("hits").getAsJsonList("hits");
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		String _scroll_id = json.getAsString("_scroll_id");
		return CcpOtherConstants.EMPTY_JSON.put("hits", collect).put("_scroll_id", _scroll_id);
	}

}
