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

import java.io.CharArrayWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * 
 * @author saniam
 * 
 *
 */
public class CsvProcessor {

	public final static List<String> CSV_TYPES = Arrays.asList(
			new String[] { "Default", "Excel", "InformixUnload", "InformixUnloadCsv", "MySQL", "RFC4180", "TDF" });

	public static List<Column> extractColumns(Schema schema, boolean isDescending, boolean isSortedByPlace) {
		List<Column> columns = new ArrayList<>();

		schema.getFields().forEach(f -> {
			System.err.println(f.getObjectProps().keySet().contains("place"));
			Column c = new Column(f, f.schema());
			c.setDefaultValue(f.defaultVal());
			try {
				c.setOrder((int) f.getObjectProps().get("place"));
			} catch (Exception e) {
				// TODO: handle exception
			}
			columns.add(c);

		});
		Collections.sort(columns, new Comparator<Column>() {

			@Override
			public int compare(Column c1, Column c2) {
				if (isDescending) {
					if (isSortedByPlace) {
						return Integer.compare(c2.order, c1.order);
					} else {
						return c2.getField().name().compareTo(c1.getField().name());
					}

				} else {
					if (isSortedByPlace) {
						return Integer.compare(c1.order, c2.order);
					} else {
						return c1.getField().name().compareTo(c2.getField().name());
					}

				}
			}
		});

		return columns;
	}

	public static String processHeaders(List<Column> columns) {
		StringBuilder sb = new StringBuilder();

		columns.forEach(c -> {
			sb.append(c.getField().name() + ":" + c.getType().getName() + ",");
		});
		sb.deleteCharAt(sb.length() - 1);

		return sb.toString();
	}

	public static String[] processHeadersAsArray(List<Column> columns) {

		return columns.stream().map(Column::getHeader).toArray(String[]::new);
	}

	public static CsvBundle generateCsvPrinter(String recordDelimiter, String compatibility) throws Exception {
		CSVFormat csvFormat = CSVFormat.valueOf(compatibility).withRecordSeparator(recordDelimiter);
		CharArrayWriter writer = new CharArrayWriter();
		CSVPrinter printer = new CSVPrinter(writer, csvFormat);
		return new CsvBundle(printer, writer);
	}

	public static List processRecord(CSVPrinter printer, GenericRecord record, List<Column> columns)
			throws IOException {
		List r = new ArrayList<>();
		columns.forEach(c -> {
			try {
				r.add(record.get(c.getField().name()));
			} catch (Exception e) {
				r.add(c.getDefaultValue());
			}
		});

		printer.printRecord(r);
		return r;
	}

	public static class CsvBundle {
		private CSVPrinter printer;
		private CharArrayWriter writer;

		public CsvBundle(CSVPrinter printer, CharArrayWriter writer) {
			super();
			this.printer = printer;
			this.writer = writer;
		}

		public CSVPrinter getPrinter() {
			return printer;
		}

		public void setPrinter(CSVPrinter printer) {
			this.printer = printer;
		}

		public CharArrayWriter getWriter() {
			return writer;
		}

		public void setWriter(CharArrayWriter writer) {
			this.writer = writer;
		}

	}

	public static class Column {
		private Field field;
		private int order;
		private Schema type;
		private Object defaultValue;

		public Column(Field field, Schema type) {
			super();
			this.field = field;
			this.type = type;
		}

		public Field getField() {
			return field;
		}

		public void setField(Field field) {
			this.field = field;
		}

		public int getOrder() {
			return order;
		}

		public void setOrder(int order) {
			this.order = order;
		}

		public Schema getType() {
			return type;
		}

		public void setType(Schema type) {
			this.type = type;
		}

		public String getHeader() {
			return this.field.name();
		}

		public Object getDefaultValue() {
			return defaultValue;
		}

		public void setDefaultValue(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public String toString() {
			return "Column [field=" + field + ", order=" + order + ", type=" + type + "]";

		}
	}
}