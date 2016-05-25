/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.boot.actuate.health;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.util.Assert;

/**
 * Simple implementation of a {@link HealthIndicator} returning status information for
 * Redis data stores.
 *
 * @author Christian Dupuis
 * @author Richard Santana
 * @since 1.1.0
 */
public class RedisHealthIndicator extends AbstractHealthIndicator {

	private static final String VERSION = "version";
	private static final String REDIS_VERSION = "redis_version";
	private final RedisConnectionFactory redisConnectionFactory;

	public RedisHealthIndicator(RedisConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		this.redisConnectionFactory = connectionFactory;
	}

	@Override
	protected void doHealthCheck(Health.Builder builder) throws Exception {
		RedisConnection connection = RedisConnectionUtils
				.getConnection(this.redisConnectionFactory);
		try {
			Properties info = connection.info();
			if (connection instanceof RedisClusterConnection) {
				redisClusterInfo(builder, info);
			}
			else {
				defaultRedisInfo(builder, VERSION, info.getProperty(REDIS_VERSION));
			}
		}
		finally {
			RedisConnectionUtils.releaseConnection(connection,
					this.redisConnectionFactory);
		}
	}

	private void defaultRedisInfo(Health.Builder builder, String key, String value) {
		builder.up().withDetail(key, value);
	}

	private void redisClusterInfo(Health.Builder builder, Properties info) {
		List<String> versionInfoList = info.keySet().stream()
				.map(actualInfo -> (String) actualInfo)
				.filter(actualInfo -> actualInfo.contains(".version"))
				.collect(Collectors.toList());
		for (String versionInfo : versionInfoList) {
			defaultRedisInfo(builder, versionInfo, info.getProperty(versionInfo));

		}
	}

}
