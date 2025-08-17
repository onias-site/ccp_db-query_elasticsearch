package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
class FunctionResponseHandlerToSearch implements Function<CcpJsonRepresentation, List<CcpJsonRepresentation>>{
	enum JsonFieldNames implements CcpJsonFieldName{
		hits
	}
	private FunctionSourceHandler handler = new FunctionSourceHandler();
	
	public List<CcpJsonRepresentation> apply(CcpJsonRepresentation json) {
		List<CcpJsonRepresentation> hits = json.getInnerJson(JsonFieldNames.hits)
				.getAsJsonList(JsonFieldNames.hits);
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		return collect;
	}
}



