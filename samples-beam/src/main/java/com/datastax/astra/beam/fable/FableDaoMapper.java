
package com.datastax.astra.beam.fable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

@Mapper
public interface FableDaoMapper {

    @DaoFactory
    FableDao getFableDao(@DaoKeyspace CqlIdentifier keyspace);

}

