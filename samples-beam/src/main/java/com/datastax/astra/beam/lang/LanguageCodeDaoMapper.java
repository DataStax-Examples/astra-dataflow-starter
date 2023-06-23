package com.datastax.astra.beam.lang;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

@Mapper
public interface LanguageCodeDaoMapper {

    @DaoFactory
    LanguageCodeDao getLanguageCodeDao(@DaoKeyspace CqlIdentifier keyspace);

}

