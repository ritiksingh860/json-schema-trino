package com.trino.jsonschematrino;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class JsonSchemaTrinoApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(JsonSchemaTrinoApplication.class, args);

        System.out.println();
        System.out.println();
        System.out.println("====================================== Schema Result Start===============================");
        JsonSchemaTrino schemaTrino = new JsonSchemaTrino();
        schemaTrino.getConfiguration(args);
        System.out.println("====================================== Schema Result End===============================");
    }
}
