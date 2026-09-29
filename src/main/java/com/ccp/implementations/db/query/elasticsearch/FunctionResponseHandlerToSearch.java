package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;
import java.util.stream.Stream;/**
 * Function that converts the raw response of an Elasticsearch {@code _search} into the list of hits,
 * applying {@code FunctionSourceHandler} to each item to extract the {@code _source} content.
 */

class FunctionResponseHandlerToSearch implements Function<CcpJsonRepresentation, List<CcpJsonRepresentation>>{
	private FunctionSourceHandler handler = new FunctionSourceHandler();

	public List<CcpJsonRepresentation> apply(CcpJsonRepresentation json) {
		CcpJsonRepresentation hitsJson = json.getInnerJson(CcpJsonCommonsFields.hits);
		List<CcpJsonRepresentation> hits = hitsJson
				.getAsJsonList(CcpJsonCommonsFields.hits);
				Stream<CcpJsonRepresentation> hitsStream = hits.stream();
				var sourcesStream = hitsStream.map(x -> this.handler.execute(x));
				List<CcpJsonRepresentation> sources = sourcesStream.collect(Collectors.toList());
		return sources;
	}
}



