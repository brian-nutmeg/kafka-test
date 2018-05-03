package com.nutmeg.kafka.producer;

import org.apache.avro.Schema;

public class ProducerSchema {

    public static Schema createSchema() {
        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                "\"name\": \"page_visit\"," +
                "\"fields\": [" +
                "{\"name\": \"time\", \"type\": \"long\"}," +
                "{\"name\": \"action\", \"type\": \"string\"}," +
                "{\"name\": \"email\", \"type\": \"string\"}," +
                "{\"name\": \"data\", \"type\": \"string\"}" +
                "]}";

        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

}
