package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
enum FunctionResponseHandlerToConsumeSearchConstants  implements CcpJsonFieldName{
	hits, _scroll_id
	
}
class FunctionResponseHandlerToConsumeSearch implements Function<CcpJsonRepresentation, CcpJsonRepresentation>{
	private FunctionSourceHandler handler = new FunctionSourceHandler();
	
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		List<CcpJsonRepresentation> hits = json.getInnerJson(FunctionResponseHandlerToConsumeSearchConstants.hits).getAsJsonList(FunctionResponseHandlerToConsumeSearchConstants.hits);
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		String _scroll_id = json.getAsString(FunctionResponseHandlerToConsumeSearchConstants._scroll_id);
		return CcpOtherConstants.EMPTY_JSON.put(FunctionResponseHandlerToConsumeSearchConstants.hits, collect).put(FunctionResponseHandlerToConsumeSearchConstants._scroll_id, _scroll_id);
	}

}
