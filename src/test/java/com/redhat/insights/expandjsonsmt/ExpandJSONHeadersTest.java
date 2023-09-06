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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ExpandJSONHeadersTest {
    private ExpandJSONHeaders<SinkRecord> xform = new ExpandJSONHeaders.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(new HashMap<>());

        final Map<String, Object> value = new HashMap<>();
        value.put("headers", "{\"stringValue\":\"Moskwa\",\"numberValue\":123}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);
        assertNull(transformedRecord);
    }

    @Test
    public void basicCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "headers");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("headers", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("headers","{\"stringValue\":\"TW9zY293\",\"numberValue\":123}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(2, updatedHeaders.size());
        assertEquals("Moscow", updatedHeaders.lastWithName("stringValue").value());
        assertEquals(123, updatedHeaders.lastWithName("numberValue").value());
    }

    @Test
    public void emptyObject() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "headers");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("headers", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("headers","{}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(0, updatedHeaders.size());
    }

    @Test
    public void complex() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "headers");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("headers", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("name", "Josef");
        value.put("age", 42);
        value.put("headers",
                "{\"stringValue\":\"TW9zY293\",\"numberValue\":123,\"complex\": { \"valueA\": 1, \"valueB\": 2 }}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(3, updatedHeaders.size());
        assertEquals("Moscow", updatedHeaders.lastWithName("stringValue").value());
        assertEquals(123, updatedHeaders.lastWithName("numberValue").value());
        assertEquals("Struct{valueA=1,valueB=2}", updatedHeaders.lastWithName("complex").value().toString());
    }

    @Test
    public void arrayCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "obj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("obj", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("obj","{\"ar1\":[1,2,3,4]}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(1, updatedHeaders.size());

        final ArrayList<Integer> actual = (ArrayList<Integer>) updatedHeaders.lastWithName("ar1").value();
        assertEquals(4, actual.size());
    }

    @Test
    public void testEmptyArray() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "obj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("obj", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(schema);
        value.put("obj","{\"null\": {\"uIczQ\": []}}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(1, updatedHeaders.size());

        final Struct struct = (Struct) updatedHeaders.lastWithName("null").value();

        assertNotNull(struct);
        assertEquals(0, struct.getArray("uIczQ").size());
    }

    @Test
    public void nullValue() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "metadata");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("age", Schema.INT32_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("age", 42);
        value.put("metadata", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(0, updatedHeaders.size());
    }

    @Test
    public void arrayInRoot() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "arr1");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("arr1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr3", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("arr1","[1,2,3,4]");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(0, updatedHeaders.size());
    }

    @Test
    public void malformed() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj2");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("obj2", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(schema);
        value.put("obj2", "{malf");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Headers updatedHeaders = transformedRecord.headers();
        assertEquals(0, updatedHeaders.size());
    }
}
