package itbi.nifi.processor.processors.hashor;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroTestUtil {

    public static ByteArrayOutputStream serializeAvroRecord(final Schema schema, final DatumWriter<GenericRecord> datumWriter, final GenericRecord... users) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, out);
            for (final GenericRecord user : users) {
                dataFileWriter.append(user);
            }
        }
        return out;
    }

}
