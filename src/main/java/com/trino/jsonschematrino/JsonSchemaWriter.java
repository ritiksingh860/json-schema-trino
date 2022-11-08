package com.trino.jsonschematrino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

@Component
public class JsonSchemaWriter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static String getJsonSchema(JsonNode properties) throws JsonProcessingException {
        ObjectNode schema = OBJECT_MAPPER.createObjectNode();
        schema.put("type", "object");

        schema.set("properties", properties);

        ObjectMapper jacksonObjectMapper = new ObjectMapper();
        String schemaString = jacksonObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
        return schemaString;
    }

    private static ObjectNode createProperty(JsonNode jsonData) throws IOException {
        ObjectNode propObject = OBJECT_MAPPER.createObjectNode();

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = jsonData.fields();

        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();

            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            JsonNodeType fieldType = fieldValue.getNodeType();

            ObjectNode property = processJsonField(fieldValue, fieldType, fieldName);
            if (!property.isEmpty()) {
                propObject.set(fieldName, property);
            }
        }
        return propObject;
    }

    private static ObjectNode processJsonField(JsonNode fieldValue, JsonNodeType fieldType, String fieldName)
            throws IOException {
        ObjectNode property = OBJECT_MAPPER.createObjectNode();

        switch (fieldType) {

            case ARRAY:
                property.put("type", "array");

                if (fieldValue.isEmpty()) {
                    break;
                }

                // Get first element of the array
                JsonNodeType typeOfArrayElements = fieldValue.get(0).getNodeType();
                if (typeOfArrayElements.equals(JsonNodeType.OBJECT)) {
                    property.set("items", createProperty(fieldValue.get(0)));
                } else {
                    property.set("items", processJsonField(fieldValue.get(0), typeOfArrayElements, fieldName));
                }
                break;
            case BOOLEAN:
                property.put("type", "boolean");
                break;

            case NUMBER:
                property.put("type", "number");
                break;

            case OBJECT:
                property.put("type", "object");
                property.set("properties", createProperty(fieldValue));
                break;

            case STRING:
                property.put("type", "string");
                break;
            default:
                break;
        }
        return property;
    }

    public static String getJsonSchemas(String jsonDocument) throws IllegalArgumentException, IOException {
        Map<String, Object> map = OBJECT_MAPPER.readValue(jsonDocument, new TypeReference<Map<String, Object>>() {});
        return getJsonSchema(map);
    }

    public static String getJsonSchema(Map<String, Object> jsonDocument) throws IllegalArgumentException, IOException {

        JsonNode properties = createProperty(OBJECT_MAPPER.convertValue(jsonDocument, JsonNode.class));
        return getJsonSchema(properties);

    }


    public static void main(String args[]) throws IllegalArgumentException, IOException {

//        String jsonPayload = null;
//        try {
//            JSONParser parser = new JSONParser();
//            //Use JSONObject for simple JSON and JSONArray for array of JSON.
//            org.json.simple.JSONObject data = (org.json.simple.JSONObject) parser.parse(
//                    new FileReader("C:\\Users\\ritik.singh\\Geany 1.38\\etc\\meta.json"));//path to the JSON file.
//            jsonPayload = data.toString();
//        } catch (IOException | ParseException e) {
//            e.printStackTrace();
//        }

//        String jsonSchema = getJsonSchema(jsonPayload);
//
//        System.out.println(jsonSchema);

//        String jsonSchema = "{\"title\":\"Schedule\",\"description\":\"this is a test\",\"type\":\"object\",\"properties\":{\"image\":{\"type\":\"object\",\"properties\":{\"height1\":{\"type\":\"number\"},\"url1\":{\"type\":\"string\"},\"width1\":{\"type\":\"number\"}}},\"thumbnail\":{\"type\":\"object\",\"properties\":{\"width\":{\"type\":\"number\"},\"url\":{\"type\":\"string\"},\"height\":{\"type\":\"number\"}}},\"name\":{\"type\":\"string\"},\"id\":{\"type\":\"string\"},\"type\":{\"type\":\"string\"}}}\n";
//        System.out.println(createQuery(jsonSchema,"test","test_table"));

    }

    public static int counter =0;

    /**
     *
     * @param schemajson	Scheam JSON
     * @param tableAlias	Table Alias
     * @param tableName		Table Name
     * @return
     */
    public String createQuery(String schemajson, String tableAlias, String tableName) {

        JSONObject metaProperties = new JSONObject(schemajson).getJSONObject("properties");
        StringBuilder select = new StringBuilder("select ");
        StringBuilder crossJoin = new StringBuilder("from " + tableName + " " + tableAlias);

        for(String modelKey : metaProperties.keySet()) {
            JSONObject modelObject = metaProperties.getJSONObject(modelKey);
            getSelectStatementFromNewSchema(modelObject, crossJoin, modelKey, null, select, tableAlias);
        }

        return getFinalSelectStatement(select) + crossJoin.toString();
    }

    private static String getFinalSelectStatement(StringBuilder select) {
        select.setCharAt(select.toString().lastIndexOf(","), Character.MIN_VALUE);
        return select.toString();
    }

    /**
     * Do not make this method static as it is using counter variable
     *
     * @param modelObject Schema JSON
     * @param crossJoin   Cross Join Query Statement
     * @param parentKey   Parent Key for the current JSON block
     * @param currentKey, Will be null for root element, only used for array type of
     *                    objects while creating alias
     * @param select,     Select Statement
     * @param tableAlias  It is used only in query processing and only for root
     *                    element
     */

    private static void getSelectStatementFromNewSchema(JSONObject modelObject, StringBuilder crossJoin, String parentKey,
                                                        String currentKey, StringBuilder select, String tableAlias) {
        JSONObject properties = null;
        if (modelObject != null) {
            if (modelObject.getString("type").equals("array")) {
                currentKey = currentKey == null ? parentKey : currentKey;
                String alias = currentKey + "_" + counter++;
                if (tableAlias != null) {
                    crossJoin.append(" CROSS JOIN UNNEST(" + tableAlias + "." + parentKey + ") " + alias);
                } else {
                    crossJoin.append(" CROSS JOIN UNNEST(" + parentKey + ") " + alias);
                }
                parentKey = alias;

                JSONObject items;
                try {
                    items = modelObject.getJSONObject("items");
                } catch (JSONException e) {
                    select.append(parentKey + ".*, ");
                    return;
                }

                for (String column : items.keySet()) {
                    try {
                        JSONObject columnObject = items.getJSONObject(column);
                        getSelectStatementFromNewSchema(columnObject, crossJoin, parentKey + "." + column, column, select, null);
                    } catch (JSONException e) {	//ARRAY OF PREMITIVE ELEMENTS
                        select.append(parentKey + ".*, ");
                    }
                }

            } else if (modelObject.getString("type").equals("object")) {
                properties = modelObject.getJSONObject("properties");
                parentKey = parentKey != null ? parentKey + "." : "";

                for (String column : properties.keySet()) {
                    JSONObject columnObject = properties.getJSONObject(column);
                    getSelectStatementFromNewSchema(columnObject, crossJoin, parentKey + column, column, select, null);
                }
            } else {
                if(tableAlias!= null){
                    select.append(tableAlias).append(".").append(parentKey + ", ");
                }else{
                    select.append(parentKey + ", ");
                }
            }
        }
    }

}
