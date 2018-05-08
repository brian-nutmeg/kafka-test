package com.nutmeg.kafka.producer;

import org.apache.avro.Schema;

public class ProducerSchema {

    public static Schema createSchema() {
        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                "\"name\": \"mytopic123\"," +
                "\"fields\": [" +
                "{\"name\": \"time\", \"type\": \"long\"}," +
                "{\"name\": \"action\", \"type\": \"string\"}," +
                "{\"name\": \"email\", \"type\": \"string\"}," +
                "{\"name\": \"data\", \"type\": [\"null\", \"string\"], \"default\": \"\"}," +
                "{\"name\": \"index\", \"type\": \"long\", \"default\": \"0\"}" +
                "]}";

        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

}
