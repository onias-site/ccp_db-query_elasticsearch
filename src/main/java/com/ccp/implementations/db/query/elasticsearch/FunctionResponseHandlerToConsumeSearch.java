package com.ccp.implementations.db.query.elasticsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.business.CcpBusiness;
import com.ccp.constants.CcpOtherConstants;
import java.util.stream.Stream;

import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * {@code CcpBusiness} that processes the first page of an Elasticsearch scroll search.
 * Extracts the list of hits (via {@code FunctionSourceHandler}) and the {@code _scroll_id} used by
 * the following pages.
 */
class FunctionResponseHandlerToConsumeSearch implements CcpBusiness{
	/** Converts each hit. */
	private FunctionSourceHandler handler = new FunctionSourceHandler();

	/**
	 * Returns the hits (converted by {@code FunctionSourceHandler}) and the {@code _scroll_id} of the page.
	 * @param json the search response
	 * @return {@code hits} and {@code _scroll_id}
	 */
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		CcpJsonRepresentation hitsJson = json.getInnerJson(CcpJsonCommonsFields.hits);
		List<CcpJsonRepresentation> hits = hitsJson.getAsJsonList(CcpJsonCommonsFields.hits);
		Stream<CcpJsonRepresentation> hitsStream = hits.stream();
		var sourcesStream = hitsStream.map(x -> this.handler.execute(x));
		List<CcpJsonRepresentation> sources = sourcesStream.collect(Collectors.toList());
		String _scroll_id = json.getAsString(CcpJsonCommonsFields._scroll_id);
		CcpJsonRepresentation pageWithHits = CcpOtherConstants.EMPTY_JSON.put(CcpJsonCommonsFields.hits, sources);
		CcpJsonRepresentation pageWithHitsAndScrollId = pageWithHits.put(CcpJsonCommonsFields._scroll_id, _scroll_id);
		return pageWithHitsAndScrollId;
	}

}
