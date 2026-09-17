package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.business.CcpBusiness;
import com.ccp.constants.CcpOtherConstants;
import java.util.stream.Stream;

import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * {@code CcpBusiness} que processa a primeira página de um scroll search do Elasticsearch.
 * Extrai a lista de hits (via {@code FunctionSourceHandler}) e o {@code _scroll_id} para uso nas
 * páginas seguintes.
 */
class FunctionResponseHandlerToConsumeSearch implements CcpBusiness{
	private FunctionSourceHandler handler = new FunctionSourceHandler();
	
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		CcpJsonRepresentation innerJson = json.getInnerJson(CcpJsonCommonsFields.hits);
		List<CcpJsonRepresentation> hits = innerJson.getAsJsonList(CcpJsonCommonsFields.hits);
		Stream<CcpJsonRepresentation> stream = hits.stream();
		var streamMap = stream.map(x -> this.handler.execute(x));
		List<CcpJsonRepresentation> collect = streamMap.collect(Collectors.toList());
		String _scroll_id = json.getAsString(CcpJsonCommonsFields._scroll_id);
		CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON.put(CcpJsonCommonsFields.hits, collect);
		CcpJsonRepresentation put2 = put.put(CcpJsonCommonsFields._scroll_id, _scroll_id);
		return put2;
	}

}
