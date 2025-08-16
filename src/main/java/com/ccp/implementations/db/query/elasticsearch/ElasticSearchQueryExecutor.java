package com.ccp.implementations.db.query.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.query.CcpDbQueryOptions;
import com.ccp.especifications.db.query.CcpQueryExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.http.CcpHttpMethods;
enum ElasticSearchQueryExecutorConstants  implements CcpJsonFieldName{
	key, value, _scroll_id, scroll, hits, scroll_id, count, _id, _source, total, aggregations, buckets, doc_count
}
class ElasticSearchQueryExecutor implements CcpQueryExecutor {
	

	
	public CcpJsonRepresentation getTermsStatis(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String fieldName) {
		CcpJsonRepresentation md = CcpOtherConstants.EMPTY_JSON;
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		
		List<CcpJsonRepresentation> asMapList = aggregations.getDynamicVersion().getAsJsonList(fieldName);
		
		for (CcpJsonRepresentation mapDecorator : asMapList) {
			String key = ""+ mapDecorator.getAsString(ElasticSearchQueryExecutorConstants.key);
			md = md.getDynamicVersion().put(key, mapDecorator.getAsLongNumber(ElasticSearchQueryExecutorConstants.value));
		}
		return md;
	}
	
	public CcpJsonRepresentation delete(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("delete", "/_delete_by_query", CcpHttpMethods.POST, 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);

		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation update(CcpDbQueryOptions elasticQuery, String[] resourcesNames, CcpJsonRepresentation newValues) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("update", "/_update_by_query", CcpHttpMethods.POST, 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);
		
		return executeHttpRequest;
	}
	
	public CcpQueryExecutor consumeQueryResult(CcpDbQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, Integer pageSize, Consumer<CcpJsonRepresentation> consumer, String... fields) {
		
		Consumer<List<CcpJsonRepresentation>> x = list -> {
			for (CcpJsonRepresentation item : list) {
				consumer.accept(item);
			}
		};
		
		CcpQueryExecutor consumeQueryResult = this.consumeQueryResult(elasticQuery, resourcesNames, scrollTime, pageSize.longValue(), x, fields);
		return consumeQueryResult;
	}	
	
	public CcpQueryExecutor consumeQueryResult(CcpDbQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, Long pageSize, Consumer<List<CcpJsonRepresentation>> consumer, String... fields) {

		long total = this.total(elasticQuery, resourcesNames);
		String indexes = this.getIndexes(resourcesNames);
	
		String scrollId = "";                  
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		for(int k = 0; k <= total; k += pageSize) {
			boolean firstPage = k == 0;
			
			if(firstPage) {
				String url = indexes + "/_search?size=" + pageSize + "&scroll="+ scrollTime;
				FunctionResponseHandlerToConsumeSearch searchDataTransform = new FunctionResponseHandlerToConsumeSearch();
				CcpJsonRepresentation flows = CcpOtherConstants.EMPTY_JSON.addJsonTransformer(200, CcpOtherConstants.DO_NOTHING).addJsonTransformer(404, CcpOtherConstants.RETURNS_EMPTY_JSON);
				CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", url, CcpHttpMethods.POST, flows,  elasticQuery.json, CcpHttpResponseType.singleRecord);
				CcpJsonRepresentation _package = searchDataTransform.apply(executeHttpRequest);
				List<CcpJsonRepresentation> hits = _package.getAsJsonList(ElasticSearchQueryExecutorConstants.hits);
				scrollId = _package.getAsString(ElasticSearchQueryExecutorConstants._scroll_id);
				consumer.accept(hits);
				continue;
			}
			
			CcpJsonRepresentation flows = CcpOtherConstants.EMPTY_JSON.addJsonTransformer(200, CcpOtherConstants.DO_NOTHING).addJsonTransformer(404, CcpOtherConstants.RETURNS_EMPTY_JSON);
			CcpJsonRepresentation scrollRequest = CcpOtherConstants.EMPTY_JSON.put(ElasticSearchQueryExecutorConstants.scroll, scrollTime).put(ElasticSearchQueryExecutorConstants.scroll_id, scrollId);
			
			FunctionResponseHandlerToSearch searchDataTransform = new FunctionResponseHandlerToSearch();
			CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", "/_search/scroll", CcpHttpMethods.POST, flows,  scrollRequest, CcpHttpResponseType.singleRecord);
			List<CcpJsonRepresentation> hits = searchDataTransform.apply(executeHttpRequest);
			consumer.accept(hits);
		}
		return this;
	}

	
	public long total(CcpDbQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String indexes = this.getIndexes(resourcesNames);
		String url = indexes + "/_count";
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getTotalRecords", url, CcpHttpMethods.POST, 200, elasticQuery.json, CcpHttpResponseType.singleRecord);
		Long count = executeHttpRequest.getAsLongNumber(ElasticSearchQueryExecutorConstants.count);
		return count;
	}

	public String getIndexes(String[] resourcesNames) {
		String indexes = "/" + Arrays.asList(resourcesNames).toString().replace("[", "").replace("]", "");
		return indexes;
	}

	
	public List<CcpJsonRepresentation> getResultAsList(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation executeHttpRequest = this.getResultAsPackage("/_search", CcpHttpMethods.POST, 200, elasticQuery, resourcesNames, fieldsToSearch);
		
		FunctionResponseHandlerToSearch searchDataTransform = new FunctionResponseHandlerToSearch();
		List<CcpJsonRepresentation> hits = searchDataTransform.apply(executeHttpRequest);
		return hits;
	}

	
	public CcpJsonRepresentation getResultAsMap(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String field) {
		List<CcpJsonRepresentation> resultAsList = this.getResultAsList(elasticQuery, resourcesNames, field);
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : resultAsList) {
			String id = md.getAsString(ElasticSearchQueryExecutorConstants._id);
			Object value = md.getDynamicVersion().get(field);
			result = result.getDynamicVersion().put(id, value);
		}
		return result;
	}

	
	public CcpJsonRepresentation getResultAsPackage(String url, CcpHttpMethods method, int expectedStatus, CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation _source = elasticQuery.json.put(ElasticSearchQueryExecutorConstants._source, Arrays.asList(fieldsToSearch));
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getResultAsPackage", url, method, expectedStatus,  _source, resourcesNames, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getMap(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String field) {
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		List<CcpJsonRepresentation> asMapList = aggregations.getDynamicVersion().getAsJsonList(field);
		CcpJsonRepresentation retorno = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : asMapList) {
			Object value = md.get(ElasticSearchQueryExecutorConstants.value);
			String key = md.getAsString(ElasticSearchQueryExecutorConstants.key);
			retorno = retorno.getDynamicVersion().put(key, value);
		}
		return retorno;
	}

	
	public CcpJsonRepresentation getAggregations(CcpDbQueryOptions elasticQuery, String... resourcesNames) {
		
		CcpJsonRepresentation resultAsPackage = this.getResultAsPackage("/_search", CcpHttpMethods.POST, 200, elasticQuery, resourcesNames);
		CcpJsonRepresentation result = getAggregations(resultAsPackage);
		
		return result;
	}

	public static CcpJsonRepresentation getAggregations(CcpJsonRepresentation resultAsPackage) {
		CcpJsonRepresentation innerJson = resultAsPackage.getInnerJson(ElasticSearchQueryExecutorConstants.total);
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON;
		boolean containsAllKeys = innerJson.containsAllFields(ElasticSearchQueryExecutorConstants.value);
		if(containsAllKeys) {
			Double total = innerJson.getAsDoubleNumber(ElasticSearchQueryExecutorConstants.value);
			result = result.put(ElasticSearchQueryExecutorConstants.total, total);			
		}
		CcpJsonRepresentation aggregations = resultAsPackage.getInnerJson(ElasticSearchQueryExecutorConstants.aggregations);
		Set<String> allAggregations = aggregations.fieldSet();
		
		for (String aggregationName : allAggregations) {
			
			CcpJsonRepresentation value = aggregations.getDynamicVersion().getInnerJson(aggregationName);
			
			boolean ignore = value.containsField(ElasticSearchQueryExecutorConstants.buckets) == false;
			
			if(ignore) {
				Double asDoubleNumber = value.getAsDoubleNumber(ElasticSearchQueryExecutorConstants.value);
				result = result.getDynamicVersion().put(aggregationName, asDoubleNumber);
				continue;
			}
			List<CcpJsonRepresentation> results = value.getAsJsonList(ElasticSearchQueryExecutorConstants.buckets);
			
			for (CcpJsonRepresentation object : results) {
				String key = object.getAsString(ElasticSearchQueryExecutorConstants.key);
				Double asDoubleNumber = object.getAsDoubleNumber(ElasticSearchQueryExecutorConstants.doc_count);
				result = result.getDynamicVersion().addToItem(aggregationName, key, asDoubleNumber);
			}
		}
		return result;
	}


}
