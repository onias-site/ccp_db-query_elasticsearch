package com.ccp.implementations.db.query.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.query.CcpDbQueryOptions;
import com.ccp.especifications.db.query.CcpQueryExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.http.CcpHttpMethods;

class ElasticSearchQueryExecutor implements CcpQueryExecutor {
	

	
	public CcpJsonRepresentation getTermsStatis(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String fieldName) {
		CcpJsonRepresentation md = CcpOtherConstants.EMPTY_JSON;
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		
		List<CcpJsonRepresentation> asMapList = aggregations.getAsJsonList(fieldName);
		
		for (CcpJsonRepresentation mapDecorator : asMapList) {
			String key = ""+ mapDecorator.getAsString("key");
			md = md.put(key, mapDecorator.getAsLongNumber("value"));
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
				CcpJsonRepresentation flows = CcpOtherConstants.EMPTY_JSON.addJsonTransformer("200", CcpOtherConstants.DO_NOTHING).addJsonTransformer("404", CcpOtherConstants.RETURNS_EMPTY_JSON);
				CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("consumeQueryResult", url, CcpHttpMethods.POST, flows,  elasticQuery.json, CcpHttpResponseType.singleRecord);
				CcpJsonRepresentation _package = searchDataTransform.apply(executeHttpRequest);
				List<CcpJsonRepresentation> hits = _package.getAsJsonList("hits");
				scrollId = _package.getAsString("_scroll_id");
				consumer.accept(hits);
				continue;
			}
			
			CcpJsonRepresentation flows = CcpOtherConstants.EMPTY_JSON.addJsonTransformer("200", CcpOtherConstants.DO_NOTHING).addJsonTransformer("404", CcpOtherConstants.RETURNS_EMPTY_JSON);
			CcpJsonRepresentation scrollRequest = CcpOtherConstants.EMPTY_JSON.put("scroll", scrollTime).put("scroll_id", scrollId);
			
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
		Long count = executeHttpRequest.getAsLongNumber("count");
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
			String id = md.getAsString("_id");
			Object value = md.get(field);
			result = result.put(id, value);
		}
		return result;
	}

	
	public CcpJsonRepresentation getResultAsPackage(String url, CcpHttpMethods method, int expectedStatus, CcpDbQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation _source = elasticQuery.json.put("_source", Arrays.asList(fieldsToSearch));
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("getResultAsPackage", url, method, expectedStatus,  _source, resourcesNames, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getMap(CcpDbQueryOptions elasticQuery, String[] resourcesNames, String field) {
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		List<CcpJsonRepresentation> asMapList = aggregations.getAsJsonList(field);
		CcpJsonRepresentation retorno = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation md : asMapList) {
			Object value = md.get("value");
			String key = md.getAsString("key");
			retorno = retorno.put(key, value);
		}
		return retorno;
	}

	
	public CcpJsonRepresentation getAggregations(CcpDbQueryOptions elasticQuery, String... resourcesNames) {
		
		CcpJsonRepresentation resultAsPackage = this.getResultAsPackage("/_search", CcpHttpMethods.POST, 200, elasticQuery, resourcesNames);
		CcpJsonRepresentation result = getAggregations(resultAsPackage);
		
		return result;
	}

	public static CcpJsonRepresentation getAggregations(CcpJsonRepresentation resultAsPackage) {
		CcpJsonRepresentation innerJson = resultAsPackage.getInnerJson("total");
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON;
		boolean containsAllKeys = innerJson.containsAllFields("value");
		if(containsAllKeys) {
			Double total = innerJson.getAsDoubleNumber("value");
			result = result.put("total", total);			
		}
		CcpJsonRepresentation aggregations = resultAsPackage.getInnerJson("aggregations");
		Set<String> allAggregations = aggregations.fieldSet();
		
		for (String aggregationName : allAggregations) {
			
			CcpJsonRepresentation value = aggregations.getInnerJson(aggregationName);
			
			boolean ignore = value.containsField("buckets") == false;
			
			if(ignore) {
				Double asDoubleNumber = value.getAsDoubleNumber("value");
				result = result.put(aggregationName, asDoubleNumber);
				continue;
			}
			List<CcpJsonRepresentation> results = value.getAsJsonList("buckets");
			
			for (CcpJsonRepresentation object : results) {
				String key = object.getAsString("key");
				Double asDoubleNumber = object.getAsDoubleNumber("doc_count");
				result = result.addToItem(aggregationName, key, asDoubleNumber);
			}
		}
		return result;
	}


}
