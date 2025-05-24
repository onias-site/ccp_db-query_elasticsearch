package com.ccp.implementations.db.query.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.query.CcpQueryExecutor;

public class CcpElasticSearchQueryExecutor implements CcpInstanceProvider<CcpQueryExecutor>  {
	
	public CcpQueryExecutor getInstance() {
		return new ElasticSearchQueryExecutor();
	}

}
