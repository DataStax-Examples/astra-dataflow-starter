package com.datastax.astra.beam.fable;

/*-
 * #%L
 * Beam SDK for Astra
 * --
 * Copyright (C) 2023 DataStax
 * --
 * Licensed under the Apache License, Version 2.0
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.datastax.astra.beam.lang.LanguageCode;
import com.datastax.astra.beam.lang.LanguageCodeDao;
import com.datastax.astra.beam.lang.LanguageCodeDaoMapperBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.concurrent.CompletionStage;

public class FableDaoMapperFactoryFn implements SerializableFunction<CqlSession, AstraDbMapper<FableDto>> {

    @Override
    public AstraDbMapper<FableDto> apply(CqlSession cqlSession) {
        // Product is not serializable, so we need to use the DAO to map to a serializable object
        FableDao dao = new FableDaoMapperBuilder(cqlSession).build()
                .getFableDao(cqlSession.getKeyspace().get());

        // Mapping to Serialize
        return new AstraDbMapperDelegate(dao);
    }

    public static class AstraDbMapperDelegate implements AstraDbMapper<FableDto> {

        FableDao dao;

        public AstraDbMapperDelegate(FableDao dao) {
            this.dao = dao;
        }

        @Override
        public FableDto mapRow(Row row) {
            return new FableDto(dao.mapRow(row));
        }

        @Override
        public CompletionStage<Void> deleteAsync(FableDto entity) {
            return dao.deleteAsync(entity.toFable());
        }

        @Override
        public CompletionStage<Void> saveAsync(FableDto entity) {
            return dao.saveAsync(entity.toFable());
        }
    }


}
