package com.ccp.implementations.db.query.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpFieldName;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.query.CcpQueryOptions;
import com.ccp.especifications.db.query.CcpQueryExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpMethods;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;
import com.ccp.decorators.CcpStringDecorator;/**
 * Implementação de {@code CcpQueryExecutor} para o Elasticsearch. Suporta busca paginada via scroll
 * ({@code consumeQueryResult}), contagem ({@code total}), listagem ({@code getResultAsList}),
 * agregações ({@code getAggregations}), deleção e atualização por query.
 */

class ElasticSearchQueryExecutor implements CcpQueryExecutor {
	enum JsonFieldNames implements CcpJsonFieldName{
		key, scroll, scroll_id, count, total, aggregations, buckets, doc_count
	}

	public CcpJsonRepresentation getTermsStatis(CcpQueryOptions elasticQuery, String[] resourcesNames, String fieldName) {
		CcpJsonRepresentation md = CcpOtherConstants.EMPTY_JSON;
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		CcpFieldName ccpFieldName = new CcpFieldName(fieldName);

		List<CcpJsonRepresentation> asMapList = aggregations.getAsJsonList(ccpFieldName);

		for (CcpJsonRepresentation mapDecorator : asMapList) {
			CcpStringDecorator asStringDecorator = mapDecorator.getAsStringDecorator(JsonFieldNames.key);
			var key = asStringDecorator.jsonFieldName();
			Long asLongNumber = mapDecorator.getAsLongNumber(CcpJsonCommonsFields.value);
			md = md.put(key, asLongNumber);
		}
		return md;
	}
	
	public CcpJsonRepresentation delete(CcpQueryOptions elasticQuery, String... resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("delete", "/_delete_by_query", CcpHttpMethods.POST, 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);

		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation update(CcpQueryOptions elasticQuery, String[] resourcesNames, CcpJsonRepresentation newValues) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("update", "/_update_by_query", CcpHttpMethods.POST, 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);
		
		return executeHttpRequest;
	}
	
	public CcpQueryExecutor consumeQueryResult(CcpQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, Integer pageSize, Consumer<CcpJsonRepresentation> consumer, String... fields) {
		
		Consumer<List<CcpJsonRepresentation>> x = list -> {
			for (CcpJsonRepresentation item : list) {
				consumer.accept(item);
			}
		};
		long longValue = pageSize.longValue();

		CcpQueryExecutor consumeQueryResult = this.consumeQueryResult(elasticQuery, resourcesNames, scrollTime, longValue, x, fields);
		return consumeQueryResult;
	}	
	
	public CcpQueryExecutor consumeQueryResult(CcpQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, Long pageSize, Consumer<List<CcpJsonRepresentation>> consumer, String... fields) {

		long total = this.total(elasticQuery, resourcesNames);
		String indexes = this.getIndexes(resourcesNames);
	
		String scrollId = "";                  
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		for(int k = 0; k <= total; k += pageSize) {
			boolean firstPage = k == 0;
			
			if(firstPage) {
				String indexesMais = indexes + "/_search?size=";
				String indexesMaisMais = indexesMais + pageSize;
				String indexesMaisMaisMais = indexesMaisMais + "&scroll=";
				String url = indexesMaisMaisMais+ scrollTime;
				FunctionResponseHandlerToConsumeSearch searchDataTransform = new FunctionResponseHandlerToConsumeSearch();
				CcpJsonRepresentation addJsonTransformer = CcpOtherConstants.EMPTY_JSON.addJsonTransformer(200, CcpOtherConstants.DO_NOTHING);
				CcpJsonRepresentation flows = addJsonTransformer.addJsonTransformer(404, CcpOtherConstants.RETURNS_EMPTY_JSON);
				CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", url, CcpHttpMethods.POST, flows,  elasticQuery.json, CcpHttpResponseType.singleRecord);
				CcpJsonRepresentation _package = searchDataTransform.execute(executeHttpRequest);
				List<CcpJsonRepresentation> hits = _package.getAsJsonList(CcpJsonCommonsFields.hits);
				scrollId = _package.getAsString(CcpJsonCommonsFields._scroll_id);
				consumer.accept(hits);
				continue;
			}
			CcpJsonRepresentation addJsonTransformer2 = CcpOtherConstants.EMPTY_JSON.addJsonTransformer(200, CcpOtherConstants.DO_NOTHING);

			CcpJsonRepresentation flows = addJsonTransformer2.addJsonTransformer(404, CcpOtherConstants.RETURNS_EMPTY_JSON);
			CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.scroll, scrollTime);
			CcpJsonRepresentation scrollRequest = put.put(JsonFieldNames.scroll_id, scrollId);
			
			FunctionResponseHandlerToSearch searchDataTransform = new FunctionResponseHandlerToSearch();
			CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", "/_search/scroll", CcpHttpMethods.POST, flows,  scrollRequest, CcpHttpResponseType.singleRecord);
			List<CcpJsonRepresentation> hits = searchDataTransform.apply(executeHttpRequest);
			consumer.accept(hits);
		}
		return this;
	}

	
	public long total(CcpQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String indexes = this.getIndexes(resourcesNames);
		String url = indexes + "/_count";
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getTotalRecords", url, CcpHttpMethods.POST, 200, elasticQuery.json, CcpHttpResponseType.singleRecord);
		Long count = executeHttpRequest.getAsLongNumber(JsonFieldNames.count);
		return count;
	}

	public String getIndexes(String[] resourcesNames) {
		String toString = Arrays.asList(resourcesNames).toString();
		String toStringReplace = toString.replace("[", "");
		String toStringReplaceReplace = toStringReplace.replace("]", "");
		String indexes = "/" + toStringReplaceReplace;
		return indexes;
	}

	
	public List<CcpJsonRepresentation> getResultAsList(CcpQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation executeHttpRequest = this.getResultAsPackage("/_search", CcpHttpMethods.POST, 200, elasticQuery, resourcesNames, fieldsToSearch);
		
		FunctionResponseHandlerToSearch searchDataTransform = new FunctionResponseHandlerToSearch();
		List<CcpJsonRepresentation> hits = searchDataTransform.apply(executeHttpRequest);
		return hits;
	}

	
	public CcpJsonRepresentation getResultAsMap(CcpQueryOptions elasticQuery, String[] resourcesNames, String field) {
		List<CcpJsonRepresentation> resultAsList = this.getResultAsList(elasticQuery, resourcesNames, field);
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : resultAsList) {
			String id = md.getAsString(CcpJsonCommonsFields._id);
			CcpFieldName ccpFieldName2 = new CcpFieldName(field);
			Object value = md.get(ccpFieldName2);
			CcpFieldName ccpFieldName3 = new CcpFieldName(id);
			result = result.put(ccpFieldName3, value);
		}
		return result;
	}

	
	public CcpJsonRepresentation getResultAsPackage(String url, CcpHttpMethods method, int expectedStatus, CcpQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation _source = elasticQuery.json.put(CcpJsonCommonsFields._source, Arrays.asList(fieldsToSearch));
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getResultAsPackage", url, method, expectedStatus,  _source, resourcesNames, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getMap(CcpQueryOptions elasticQuery, String[] resourcesNames, String field) {
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		CcpFieldName ccpFieldName4 = new CcpFieldName(field);
		List<CcpJsonRepresentation> asMapList = aggregations.getAsJsonList(ccpFieldName4);
		CcpJsonRepresentation retorno = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : asMapList) {
			Object value = md.get(CcpJsonCommonsFields.value);
			String key = md.getAsString(JsonFieldNames.key);
			CcpFieldName ccpFieldName5 = new CcpFieldName(key);
			retorno = retorno.put(ccpFieldName5, value);
		}
		return retorno;
	}

	
	public CcpJsonRepresentation getAggregations(CcpQueryOptions elasticQuery, String... resourcesNames) {
		
		CcpJsonRepresentation resultAsPackage = this.getResultAsPackage("/_search", CcpHttpMethods.POST, 200, elasticQuery, resourcesNames);
		CcpJsonRepresentation result = getAggregations(resultAsPackage);
		
		return result;
	}

	public static CcpJsonRepresentation getAggregations(CcpJsonRepresentation resultAsPackage) {
		CcpJsonRepresentation innerJson = resultAsPackage.getInnerJson(JsonFieldNames.total);
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON;
		boolean containsAllKeys = innerJson.containsAllFields(CcpJsonCommonsFields.value);
		if(containsAllKeys) {
			Double total = innerJson.getAsDoubleNumber(CcpJsonCommonsFields.value);
			result = result.put(JsonFieldNames.total, total);			
		}
		CcpJsonRepresentation aggregations = resultAsPackage.getInnerJson(JsonFieldNames.aggregations);
		Set<String> allAggregations = aggregations.fieldSet();
		
		for (String aggregationName : allAggregations) {
			CcpFieldName ccpFieldName6 = new CcpFieldName(aggregationName);
		
			CcpJsonRepresentation value = aggregations.getInnerJson(ccpFieldName6);
			boolean containsField = value.containsField(JsonFieldNames.buckets);

			boolean ignore = false == containsField;

			if(ignore) {
				Double asDoubleNumber = value.getAsDoubleNumber(CcpJsonCommonsFields.value);
				CcpFieldName ccpFieldName7 = new CcpFieldName(aggregationName);
				result = result.put(ccpFieldName7, asDoubleNumber);
				continue;
			}
			List<CcpJsonRepresentation> results = value.getAsJsonList(JsonFieldNames.buckets);

			for (CcpJsonRepresentation object : results) {
				String key = object.getAsString(JsonFieldNames.key);
				Double asDoubleNumber = object.getAsDoubleNumber(JsonFieldNames.doc_count);
				CcpFieldName ccpFieldName8 = new CcpFieldName(aggregationName);
				CcpFieldName ccpFieldName9 = new CcpFieldName(key);
				result = result.addToItem(ccpFieldName8, ccpFieldName9, asDoubleNumber);
			}
		}
		return result;
	}


}
