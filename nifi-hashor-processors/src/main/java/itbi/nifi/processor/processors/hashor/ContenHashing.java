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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

//import org.apache.nifi.security.util.crypto.HashAlgorithm;
//import org.apache.nifi.security.util.crypto.HashService;


import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static itbi.nifi.processor.processors.hashor.CsvProcessor.*;

@Tags({"hashing", "sha"})
@CapabilityDescription("Hash a column from a content pointed by flowfile")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ContenHashing extends AbstractProcessor {

    public static final PropertyDescriptor FIELD_NAME = new PropertyDescriptor
            .Builder().name("FIELD_NAME")
            .displayName("Fields to hash")
            .description("Respective hashing algorithm will be applied on your selected fields separated by comma.")
            .required(true)
            //.defaultValue()
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FAIL_WHEN_EMPTY = new PropertyDescriptor.Builder()
            .name("fail_when_empty")
            .displayName("Fail if the content is empty")
            .description("Route to failure if the content is empty. " +
                    "While hashing an empty value is valid, some flows may want to detect empty input.")
            .allowableValues("true", "false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash_algorithm")
            .displayName("Hash Algorithm")
            .description("The hash algorithm to use. Note that not all of the algorithms available are recommended for use (some are provided for legacy compatibility). " +
                    "There are many things to consider when picking an algorithm; it is recommended to use the most secure algorithm possible.")
            .required(true)
            .allowableValues("MD2", "MD5", "SHA224", "SHA256", "SHA512")
            .defaultValue("SHA224")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor OUTPUT_TYPE = new PropertyDescriptor.Builder()
            .name("output_type")
            .displayName("Output type")
            .description("Output type support is csv or avro")
            .required(true)
            .allowableValues("csv", "avro")
            .defaultValue("avro")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Used for flowfiles that have a hash value added")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Used for flowfiles that have no content if the 'fail on empty' setting is enabled")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(FIELD_NAME);
        descriptors.add(FAIL_WHEN_EMPTY);
        descriptors.add(HASH_ALGORITHM);
        descriptors.add(OUTPUT_TYPE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    private static final byte[] EMPTY_JSON_OBJECT = "{}".getBytes(StandardCharsets.UTF_8);
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final ComponentLog logger = getLogger();

        // Determine the algorithm to use
        final String algorithmName = context.getProperty(HASH_ALGORITHM).getValue();
        logger.debug("Using algorithm {}", new Object[]{algorithmName});

        //Determine the columns to hash
        String[] fieldsToHash = context.getProperty(FIELD_NAME).getValue().trim().split("\\s*,\\s*");

        //Determine the output type
        String outputType = context.getProperty(OUTPUT_TYPE).getValue();

        if (flowFile.getSize() == 0) {
            if (context.getProperty(FAIL_WHEN_EMPTY).asBoolean()) {
                logger.info("Routing {} to 'failure' because content is empty (and FAIL_WHEN_EMPTY is true)");
                session.transfer(flowFile, REL_FAILURE);
                return;
            } else {
                logger.debug("Flowfile content is empty; hashing with {} anyway", new Object[]{algorithmName});
            }
        }

        // Generate a hash with the configured algorithm for the content
        // and create a new attribute with the configured name
        logger.debug("Generating {} hash of content", new Object[]{algorithmName});

        try {
            final StopWatch stopWatch = new StopWatch(true);
            //Write to content file
            // This uses a closure acting as a StreamCallback to do the writing of the new content to the flowfile
                final boolean useContainer = false;
                final boolean wrapSingleRecord = true;
                flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    if(outputType == "avro") {
                        final GenericData genericData = GenericData.get();
                        GenericRecord record = null;
                        try (final InputStream in = new BufferedInputStream(rawIn);
                             final OutputStream out = new BufferedOutputStream(rawOut);
                             final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>())) {
                            Schema schema = reader.getSchema();
                            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
                            dataFileWriter.create(schema, out);

                            while(reader.hasNext()){
                                record = reader.next(record);
                                GenericRecord out_record = new GenericData.Record(schema);

                                for (Schema.Field field: schema.getFields()
                                ) {
                                    boolean hash_flag = false;
                                    for (String field_to_hash:fieldsToHash
                                    ) {
                                        if(field.name().equals(field_to_hash)){
                                            hash_flag = true;
                                            break;
                                        }
                                    }
                                    if(hash_flag){
                                        out_record.put(field.name(), getHash(record.get(field.name()).toString().getBytes(), algorithmName));
                                    }
                                    else{
                                        out_record.put(field.name(), record.get(field.name()));
                                    }
                                }
                                dataFileWriter.append(out_record);
                            }
                            dataFileWriter.close();
                        } catch (Exception e) {
                            //logger.error("Failed to handling record during hashing process", e);
                            throw new RuntimeException("Hashing avro record error: " + e.getMessage());
                        }
                    }
                    else if (outputType == "csv"){
                    //TODO: Handling output csv format file
                        try {
                            final String csvCompatibility = new AllowableValue("Default", "Default/Basic",
                                    "Default Format").getValue();

                            final String csvRecordDelimiter = "\n";
                            final String csvSortDirection = "A";    //A: Ascending; D: Descending
                            final String csvSortFIELD = "F";        //F: Field Name, P: Place attribute
                            CsvBundle bundle = generateCsvPrinter(csvRecordDelimiter, csvCompatibility);
                            try (final InputStream in = new BufferedInputStream(rawIn);
                                 final OutputStream out = new BufferedOutputStream(rawOut);
                                 final DataFileStream<GenericRecord> reader = new DataFileStream<>(in,
                                         new GenericDatumReader<GenericRecord>())) {
                                List<Column> columns = extractColumns(reader.getSchema(),
                                        csvSortDirection.equals("D"), csvSortFIELD.equals("F"));

                                GenericRecord record = null;
                                while (reader.hasNext()) {
                                    record = reader.next(record);
                                    processRecord(bundle.getPrinter(), record, columns);
//                                    out.write(bundle.getWriter().toString().getBytes());
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Hashing csv record error: " + e.getMessage());
                        }
                    }
                }
            });

            // Update provenance and route to success
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));

            if(outputType == "csv"){
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "test/csv");
            }
            session.transfer(flowFile, REL_SUCCESS);

        } catch (ProcessException e) {
            logger.error("Failed to process {} due to {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    //Available algorithms: MD2, MD5, SHA-224, SHA-256, SHA-384, SHA-512
    public static String getHash(byte[] inputBytes, String algorithm) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
        messageDigest.update(inputBytes);
        byte[] digestedBytes = messageDigest.digest();
        return DatatypeConverter.printHexBinary(digestedBytes).toLowerCase();
    }
}
