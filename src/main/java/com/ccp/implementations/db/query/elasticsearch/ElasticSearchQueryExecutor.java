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
 * {@code CcpQueryExecutor} implementation for Elasticsearch. Supports paginated scroll search
 * ({@code consumeQueryResult}), counting ({@code total}), listing ({@code getResultAsList}),
 * aggregations ({@code getAggregations}), and delete and update by query.
 */

class ElasticSearchQueryExecutor implements CcpQueryExecutor {
	enum JsonFieldNames implements CcpJsonFieldName{
		key, scroll, scroll_id, count, total, aggregations, buckets, doc_count
	}

	public CcpJsonRepresentation getTermsStatis(CcpQueryOptions elasticQuery, String[] resourcesNames, String fieldName) {
		CcpJsonRepresentation termsStatistics = CcpOtherConstants.EMPTY_JSON;
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		CcpFieldName aggregationFieldName = new CcpFieldName(fieldName);

		List<CcpJsonRepresentation> aggregationItems = aggregations.getAsJsonList(aggregationFieldName);

		for (CcpJsonRepresentation aggregationItem : aggregationItems) {
			CcpStringDecorator keyDecorator = aggregationItem.getAsStringDecorator(JsonFieldNames.key);
			var key = keyDecorator.jsonFieldName();
			Long bucketValue = aggregationItem.getAsLongNumber(CcpJsonCommonsFields.value);
			termsStatistics = termsStatistics.put(key, bucketValue);
		}
		return termsStatistics;
	}
	
	public CcpJsonRepresentation delete(CcpQueryOptions elasticQuery, String... resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("delete", "/_delete_by_query", CcpHttpMethods.POST, 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);

		return response;
	}

	
	public CcpJsonRepresentation update(CcpQueryOptions elasticQuery, String[] resourcesNames, CcpJsonRepresentation newValues) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("update", "/_update_by_query", CcpHttpMethods.POST, 200, elasticQuery.json,  resourcesNames, CcpHttpResponseType.singleRecord);
		
		return response;
	}
	
	public CcpQueryExecutor consumeQueryResult(CcpQueryOptions elasticQuery, String[] resourcesNames,
			String scrollTime, Integer pageSize, Consumer<CcpJsonRepresentation> consumer, String... fields) {
		
		Consumer<List<CcpJsonRepresentation>> pageConsumer = list -> {
			for (CcpJsonRepresentation item : list) {
				consumer.accept(item);
			}
		};
		long pageSizeAsLong = pageSize.longValue();

		CcpQueryExecutor queryExecutor = this.consumeQueryResult(elasticQuery, resourcesNames, scrollTime, pageSizeAsLong, pageConsumer, fields);
		return queryExecutor;
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
				String searchUrlWithSizeParam = indexes + "/_search?size=";
				String searchUrlWithSize = searchUrlWithSizeParam + pageSize;
				String searchUrlWithScrollParam = searchUrlWithSize + "&scroll=";
				String url = searchUrlWithScrollParam+ scrollTime;
				FunctionResponseHandlerToConsumeSearch searchDataTransform = new FunctionResponseHandlerToConsumeSearch();
				CcpJsonRepresentation handlersFor200 = CcpOtherConstants.EMPTY_JSON.addJsonTransformer(200, CcpOtherConstants.DO_NOTHING);
				CcpJsonRepresentation flows = handlersFor200.addJsonTransformer(404, CcpOtherConstants.RETURNS_EMPTY_JSON);
				CcpJsonRepresentation response = dbUtils.executeHttpRequest("consumeQueryResult", url, CcpHttpMethods.POST, flows,  elasticQuery.json, CcpHttpResponseType.singleRecord);
				CcpJsonRepresentation firstPageResult = searchDataTransform.execute(response);
				List<CcpJsonRepresentation> hits = firstPageResult.getAsJsonList(CcpJsonCommonsFields.hits);
				scrollId = firstPageResult.getAsString(CcpJsonCommonsFields._scroll_id);
				consumer.accept(hits);
				continue;
			}
			CcpJsonRepresentation handlersFor200 = CcpOtherConstants.EMPTY_JSON.addJsonTransformer(200, CcpOtherConstants.DO_NOTHING);

			CcpJsonRepresentation flows = handlersFor200.addJsonTransformer(404, CcpOtherConstants.RETURNS_EMPTY_JSON);
			CcpJsonRepresentation scrollRequestWithTime = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.scroll, scrollTime);
			CcpJsonRepresentation scrollRequest = scrollRequestWithTime.put(JsonFieldNames.scroll_id, scrollId);
			
			FunctionResponseHandlerToSearch searchDataTransform = new FunctionResponseHandlerToSearch();
			CcpJsonRepresentation response = dbUtils.executeHttpRequest("consumeQueryResult", "/_search/scroll", CcpHttpMethods.POST, flows,  scrollRequest, CcpHttpResponseType.singleRecord);
			List<CcpJsonRepresentation> hits = searchDataTransform.apply(response);
			consumer.accept(hits);
		}
		return this;
	}

	
	public long total(CcpQueryOptions elasticQuery, String[] resourcesNames) {
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String indexes = this.getIndexes(resourcesNames);
		String url = indexes + "/_count";
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getTotalRecords", url, CcpHttpMethods.POST, 200, elasticQuery.json, CcpHttpResponseType.singleRecord);
		Long count = response.getAsLongNumber(JsonFieldNames.count);
		return count;
	}

	public String getIndexes(String[] resourcesNames) {
		String resourcesNamesAsText = Arrays.asList(resourcesNames).toString();
		String namesWithoutOpeningBracket = resourcesNamesAsText.replace("[", "");
		String commaSeparatedNames = namesWithoutOpeningBracket.replace("]", "");
		String indexes = "/" + commaSeparatedNames;
		return indexes;
	}

	
	public List<CcpJsonRepresentation> getResultAsList(CcpQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation response = this.getResultAsPackage("/_search", CcpHttpMethods.POST, 200, elasticQuery, resourcesNames, fieldsToSearch);
		
		FunctionResponseHandlerToSearch searchDataTransform = new FunctionResponseHandlerToSearch();
		List<CcpJsonRepresentation> hits = searchDataTransform.apply(response);
		return hits;
	}

	
	public CcpJsonRepresentation getResultAsMap(CcpQueryOptions elasticQuery, String[] resourcesNames, String field) {
		List<CcpJsonRepresentation> resultAsList = this.getResultAsList(elasticQuery, resourcesNames, field);
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation record : resultAsList) {
			String id = record.getAsString(CcpJsonCommonsFields._id);
			CcpFieldName requestedField = new CcpFieldName(field);
			Object value = record.get(requestedField);
			CcpFieldName idField = new CcpFieldName(id);
			result = result.put(idField, value);
		}
		return result;
	}

	
	public CcpJsonRepresentation getResultAsPackage(String url, CcpHttpMethods method, int expectedStatus, CcpQueryOptions elasticQuery, String[] resourcesNames, String... fieldsToSearch) {
		CcpJsonRepresentation queryWithSourceFields = elasticQuery.json.put(CcpJsonCommonsFields._source, Arrays.asList(fieldsToSearch));
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		
		CcpJsonRepresentation response = dbUtils.executeHttpRequest("getResultAsPackage", url, method, expectedStatus,  queryWithSourceFields, resourcesNames, CcpHttpResponseType.singleRecord);
		return response;
	}

	
	public CcpJsonRepresentation getMap(CcpQueryOptions elasticQuery, String[] resourcesNames, String field) {
		CcpJsonRepresentation aggregations = this.getAggregations(elasticQuery, resourcesNames);
		CcpFieldName aggregationFieldName = new CcpFieldName(field);
		List<CcpJsonRepresentation> aggregationItems = aggregations.getAsJsonList(aggregationFieldName);
		CcpJsonRepresentation aggregationValuesByKey = CcpOtherConstants.EMPTY_JSON;
		for (CcpJsonRepresentation aggregationItem : aggregationItems) {
			Object value = aggregationItem.get(CcpJsonCommonsFields.value);
			String key = aggregationItem.getAsString(JsonFieldNames.key);
			CcpFieldName keyField = new CcpFieldName(key);
			aggregationValuesByKey = aggregationValuesByKey.put(keyField, value);
		}
		return aggregationValuesByKey;
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
			CcpFieldName aggregationField = new CcpFieldName(aggregationName);
		
			CcpJsonRepresentation value = aggregations.getInnerJson(aggregationField);
			boolean containsField = value.containsField(JsonFieldNames.buckets);

			boolean hasNoBuckets = false == containsField;

			if(hasNoBuckets) {
				Double asDoubleNumber = value.getAsDoubleNumber(CcpJsonCommonsFields.value);
				CcpFieldName aggregationResultField = new CcpFieldName(aggregationName);
				result = result.put(aggregationResultField, asDoubleNumber);
				continue;
			}
			List<CcpJsonRepresentation> buckets = value.getAsJsonList(JsonFieldNames.buckets);

			for (CcpJsonRepresentation bucket : buckets) {
				String key = bucket.getAsString(JsonFieldNames.key);
				Double asDoubleNumber = bucket.getAsDoubleNumber(JsonFieldNames.doc_count);
				CcpFieldName aggregationResultField = new CcpFieldName(aggregationName);
				CcpFieldName bucketKeyField = new CcpFieldName(key);
				result = result.addToItem(aggregationResultField, bucketKeyField, asDoubleNumber);
			}
		}
		return result;
	}


}
