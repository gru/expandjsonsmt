/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main project class implementing JSON string to headers transformation.
 */
abstract class ExpandJSONHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpandJSONHeaders.class);

    interface ConfigName {
        String SOURCE_FIELD = "sourceField";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELD, ConfigDef.Type.STRING, "headers", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to headers.");

    private static final String PURPOSE = "json field to headers expansion";

    private String sourceField;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceField = config.getString(ConfigName.SOURCE_FIELD);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            LOGGER.info("Schemaless records not supported");
            return null;
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        try {
            Object recordValue = operatingValue(record);
            if (recordValue == null) {
                LOGGER.info("ExpandJSONHeaders record is null");
                LOGGER.info(record.toString());
                return record;
            }

            String delimiterSplit = "\\.";
            final Struct value = requireStruct(recordValue, PURPOSE);
            final BsonDocument document = parseJsonField(value, sourceField, delimiterSplit);

            if (document == null) {
                LOGGER.info(String.format("ExpandJSONHeaders '%s' value is null", sourceField));
                LOGGER.info(record.toString());
                return record;
            }

            Headers updatedHeaders = record.headers().duplicate();

            for (String documentKey : document.keySet())
            {
                final BsonValue documentValue = document.get(documentKey);
                final Schema headerSchema = SchemaParser.bsonValue2Schema(documentValue);
                if (headerSchema != null) {
                    final Object headerValue = DataConverter.bsonValue2Object(documentValue, headerSchema);
                    if (headerValue != null) {
                        final SchemaAndValue literalValue = new SchemaAndValue(headerSchema, headerValue);
                        updatedHeaders.add(documentKey, literalValue);
                    } else {
                        LOGGER.warn(String.format("ExpandJSONHeaders unable to get header value: '%s' : '%s'", documentKey, documentValue));
                    }
                } else {
                    LOGGER.warn(String.format("ExpandJSONHeaders unable to get header schema: '%s' : '%s'", documentKey, documentValue));
                }
            }

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
        } catch (DataException e) {
            LOGGER.warn("ExpandJSONHeaders fields missing from record: " + record.toString(), e);
            return record;
        }
    }

    private static String getStringValue(List<String> path, Struct value) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return value.getString(path.get(0));
        } else {
            return getStringValue(path.subList(1, path.size()), value.getStruct(path.get(0)));
        }
    }

    /**
     * Parse JSON object from given string field.
     * @param value Input record to read original string field.
     * @param sourceField Source field to parse JSON objects from.
     * @return Parsed JSON object.
     */
    private static BsonDocument parseJsonField(Struct value, String sourceField, String levelDelimiter) {
        BsonDocument val;
        String[] pathArr = sourceField.split(levelDelimiter);
        List<String> path = Arrays.asList(pathArr);
        final String jsonString = getStringValue(path, value);
        if (jsonString == null) {
            val = null;
        } else {
            try {
                if (jsonString.startsWith("{")) {
                    val = BsonDocument.parse(jsonString);
                } else {
                    String msg = String.format("Unable to parse field '%s' starting with '%s'", sourceField, jsonString.charAt(0));
                    throw new Exception(msg);
                }
            } catch (Exception ex) {
                LOGGER.warn(ex.getMessage(), ex);
                val = null;
            }
        }
        return val;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() { }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    public static class Value<R extends ConnectRecord<R>> extends ExpandJSONHeaders<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
    }
}
