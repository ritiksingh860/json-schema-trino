package com.trino.jsonschematrino;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

@PropertySource(value = "classpath:application.properties")
@Service
public class JsonSchemaTrino {
    @Value("${spring.filepath}")
    public String filePath;

    @Value("${spring.filename}")
    public String fileName;

    @Value("${spring.tablename}")
    public String tableName;

    static void help() {
        System.out.println("Usage: Two arguments possible. First is required. Second is optional");
        System.out.println("  1st arg: path to JSON file to parse into Hive schema");
        System.out.println("  2nd arg (optional): tablename.  Defaults to 'x'");
    }

    public void getConfiguration(String[] args) throws Exception {
        String json = null;
        System.out.println("path = " + filePath + fileName);
        try {
            JSONParser parser = new JSONParser();
//            Use JSONObject for simple JSON and JSONArray for array of JSON.
            org.json.simple.JSONObject data = (org.json.simple.JSONObject) parser.
                    parse(new FileReader("C:\\Users\\ritik.singh\\Geany 1.38\\etc\\meta.json"));//path to the JSON file.
            json = data.toString();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        JsonSchemaTrino schema = new JsonSchemaTrino("minio.test.abc");
        System.out.println("trino schema create query ==> " + schema.createHiveSchema(json));
        JsonSchemaWriter schemaWriter = new JsonSchemaWriter();
        String jsonSchema = schemaWriter.getJsonSchemas(json);
        System.out.println("Json Schema Definition ==> " + jsonSchema);
        System.out.println("Select Query for the JSON ==> " + schemaWriter.createQuery(jsonSchema, "alias", tableName));
        JSONUtilsSchema utils = new JSONUtilsSchema();
        utils.getDefaultJson(json);
    }


    public static int counter = 0;

    public JsonSchemaTrino() {
    }

    public JsonSchemaTrino(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Pass in any valid JSON object and a Hive schema will be returned for it.
     * You should avoid having null values in the JSON document, however.
     * <p>
     * The Hive schema columns will be printed in alphabetical order - overall and
     * within subsections.
     *
     * @param json
     * @return string Hive schema
     * @throws org.json.test.JSONException if the JSON does not parse correctly
     */
    public String createHiveSchema(String json) throws org.json.test.JSONException {
        org.json.test.JSONObject jo = null;
        try {
            jo = new org.json.test.JSONObject(json);
        } catch (Exception ex) {
            json = "{\"a\":" + json + "}";
            jo = new org.json.test.JSONObject(json);
        }

        @SuppressWarnings("unchecked")
        Iterator<String> keys = jo.keys();
        keys = new OrderedIterator(keys);
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");

        while (keys.hasNext()) {
            String k = keys.next();
            sb.append("  ");
            sb.append(k);
            sb.append(' ');
            sb.append(valueToHiveSchema(jo.opt(k)));
            sb.append(',').append(" ");
        }

        sb.replace(sb.length() - 2, sb.length(), ")"); // remove last comma
        return sb.append(" WITH (external_location = ").append("'").append("filepath").append("'").append(",format = 'JSON');").toString();
    }

    private String toHiveSchema(org.json.test.JSONObject o) throws org.json.test.JSONException {
        @SuppressWarnings("unchecked")
        Iterator<String> keys = o.keys();
        keys = new OrderedIterator(keys);
        StringBuilder sb = new StringBuilder("ROW(");

        while (keys.hasNext()) {
            String k = keys.next();
            sb.append(k);
            sb.append(' ');
            sb.append(valueToHiveSchema(o.opt(k)));
            sb.append(", ");
        }
        sb.replace(sb.length() - 2, sb.length(), ")"); // remove last comma
        return sb.toString();
    }

    private String toHiveSchema(org.json.test.JSONArray a) throws org.json.test.JSONException {
        return "ARRAY(" + arrayJoin(a, ",") + ')';
    }

    private String arrayJoin(org.json.test.JSONArray a, String separator) throws org.json.test.JSONException {
        StringBuilder sb = new StringBuilder();

        if (a.length() == 0) {
            sb.append("varchar");
//      throw new IllegalStateException("Array is empty: " + a.toString());
        }
        if (a.length() != 0) {
            Object entry0 = a.get(0);
            if (isScalar(entry0)) {
//        sb.append(scalarType(entry0));
                sb.append(getType(a, separator));
            } else if (entry0 instanceof org.json.test.JSONObject) {
                sb.append(toHiveSchema((org.json.test.JSONObject) entry0));
            } else if (entry0 instanceof org.json.test.JSONArray) {
                sb.append(toHiveSchema((org.json.test.JSONArray) entry0));
            }
        }
        return sb.toString();

    }

    private String getType(org.json.test.JSONArray a, String separator) throws org.json.test.JSONException {
        Object entry0 = a.get(counter);
        String type = scalarType(entry0);
        if (type == null) {
            counter++;
            getType(a, separator);
        }
        System.out.println("type === > " + type);
        return type;
    }

    private String scalarType(Object o) {
        if (o instanceof String) return "varchar";
        if (o instanceof Number) return scalarNumericType(o);
        if (o instanceof Boolean) return "boolean";

        return null;
    }

    private String scalarNumericType(Object o) {
        String s = o.toString();
        if (s.indexOf('.') > 0) {
            return "double";
        } else {
            return "integer";
        }
    }

    private boolean isScalar(Object o) {
        return o instanceof String ||
                o instanceof Number ||
                o instanceof Boolean ||
                o == org.json.test.JSONObject.NULL;
    }

    private String valueToHiveSchema(Object o) throws org.json.test.JSONException {
        if (isScalar(o)) {
            return scalarType(o);
        } else if (o instanceof org.json.test.JSONObject) {
            return toHiveSchema((org.json.test.JSONObject) o);
        } else if (o instanceof org.json.test.JSONArray) {
            return toHiveSchema((org.json.test.JSONArray) o);
        } else {
            throw new IllegalArgumentException("unknown type: " + o.getClass());
        }
    }

    static class OrderedIterator implements Iterator<String> {

        Iterator<String> it;

        public OrderedIterator(Iterator<String> iter) {
            SortedSet<String> keys = new TreeSet<String>();
            while (iter.hasNext()) {
                String str = iter.next();
                keys.add(str);
            }
            it = keys.iterator();
        }

        public boolean hasNext() {
            return it.hasNext();
        }

        public String next() {
            return it.next();
        }

        public void remove() {
            it.remove();
        }
    }
}



