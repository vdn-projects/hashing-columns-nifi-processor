/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package itbi.nifi.processor.processors.hashor;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class ContenHashingTest {
    @Test
    public void testSingleAvroMessage() throws IOException {
//        final TestRunner runner = TestRunners.newTestRunner(new ContenHashing());
//        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
//
//        final GenericRecord user1 = new GenericData.Record(schema);
//        user1.put("name", "Alyssa");
//        user1.put("favorite_number", 256);
//
//        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
//        final ByteArrayOutputStream out1 = AvroTestUtil.serializeAvroRecord(schema, datumWriter, user1);
//        runner.enqueue(out1.toByteArray());
//
//        runner.run();
//
//        runner.assertAllFlowFilesTransferred(ContenHashing.REL_SUCCESS, 1);
//        final MockFlowFile out = runner.getFlowFilesForRelationship(ContenHashing.REL_SUCCESS).get(0);
//        out.assertContentEquals("[{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": null}]");
    }


}


