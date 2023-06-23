package com.datastax.astra.beam.fable;

import com.datastax.astra.beam.lang.LanguageCode;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;

@Dao
public interface FableDao extends AstraDbMapper<Fable> {}
