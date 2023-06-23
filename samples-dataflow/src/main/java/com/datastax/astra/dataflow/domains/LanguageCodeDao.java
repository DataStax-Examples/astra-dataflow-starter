package com.datastax.astra.dataflow.domains;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;

@Dao
public interface LanguageCodeDao extends AstraDbMapper<LanguageCode> {}
