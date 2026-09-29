package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.query.CcpQueryExecutor;

/**
 * DI provider that exposes {@code ElasticSearchQueryExecutor} as the {@code CcpQueryExecutor} implementation.
 */
public class CcpElasticSearchQueryExecutor implements CcpInstanceProvider<CcpQueryExecutor>  {

	public CcpQueryExecutor getInstance() {
		ElasticSearchQueryExecutor elasticSearchQueryExecutor = new ElasticSearchQueryExecutor();
		return elasticSearchQueryExecutor;
	}

}
