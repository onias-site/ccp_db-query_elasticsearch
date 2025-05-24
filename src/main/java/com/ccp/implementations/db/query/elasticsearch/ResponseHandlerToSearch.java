package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;

class ResponseHandlerToSearch implements Function<CcpJsonRepresentation, List<CcpJsonRepresentation>>{
	private CcpSourceHandler handler = new CcpSourceHandler();
	
	public List<CcpJsonRepresentation> apply(CcpJsonRepresentation json) {
		List<CcpJsonRepresentation> hits = json.getInnerJson("hits").getAsJsonList("hits");
		List<CcpJsonRepresentation> collect = hits.stream().map(x -> this.handler.apply(x)).collect(Collectors.toList());
		return collect;
	}
}



