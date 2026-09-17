package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;
import java.util.stream.Stream;/**
 * Função que converte a resposta bruta de um {@code _search} do Elasticsearch na lista de hits,
 * aplicando {@code FunctionSourceHandler} a cada item para extrair o conteúdo de {@code _source}.
 */

class FunctionResponseHandlerToSearch implements Function<CcpJsonRepresentation, List<CcpJsonRepresentation>>{
	private FunctionSourceHandler handler = new FunctionSourceHandler();
	
	public List<CcpJsonRepresentation> apply(CcpJsonRepresentation json) {
		CcpJsonRepresentation innerJson = json.getInnerJson(CcpJsonCommonsFields.hits);
		List<CcpJsonRepresentation> hits = innerJson
				.getAsJsonList(CcpJsonCommonsFields.hits);
				Stream<CcpJsonRepresentation> stream = hits.stream();
				var streamMap = stream.map(x -> this.handler.execute(x));
				List<CcpJsonRepresentation> collect = streamMap.collect(Collectors.toList());
		return collect;
	}
}



