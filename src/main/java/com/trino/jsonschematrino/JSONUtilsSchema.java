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
import java.util.*;

@Component
public class JSONUtilsSchema {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static int counter = 0;

    private static List<String> invalidKeyList = new ArrayList<>();

    public void getDefaultJson(String data) throws JsonProcessingException {
        JSONObject obj = null;
        String payload = null;
//        JSONParser parser = new JSONParser();
        try {
//            Object data = parser.parse(
//                    new FileReader("C:\\Users\\ritik.singh\\Geany 1.38\\testarray.json"));

            try{
                obj = new JSONObject(data);
            }catch (Exception ex){
                data = "{\"a\":" + data + "}";
                obj = new JSONObject(data);
            }
            payload = obj.toString();
//            if (obj instanceof org.json.JSONArray) {
//                JSONArray arr = (JSONArray) data;
//                JSONArray array = new JSONArray();
//                array.add(getArrayIndexData(arr));
//                payload = getArrayIndexData(arr).toString();
//            } else {
//                obj = (JSONObject) data;
//                payload = obj.toString();
//            }
//            System.out.println(payload);
            String prop = getJsonSchema(payload);
            System.out.println("Default Json ===> "+ getSampleJson(prop));
            System.out.println("Null value key path ==> "+getNullValueKeyPath(prop));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private static Object getArrayIndexData(JSONArray arr) {
//        Object obj = arr.get(counter);
//        if (obj != null) {
//            return obj;
//        } else {
//            counter++;
//            return getArrayIndexData(arr);
//        }
//    }


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
                    fieldValue = null;
                    invalidKeyList.add(fieldName);
                    property.put("value", fieldValue);
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
                property.put("value", fieldValue);
                break;

            case NUMBER:
                property.put("type", "number");
                property.put("value", fieldValue);
                break;

            case OBJECT:
                ObjectNode objectNode = createProperty(fieldValue);
                if (objectNode.isEmpty()) {
                    objectNode = null;
                    invalidKeyList.add(fieldName);
                    property.put("type", "nullObject");
                    property.set("properties", objectNode);
                } else {
                    property.put("type", "object");
                    property.set("properties", objectNode);
                }
                break;

            case STRING:
                property.put("type", "string");
                property.put("value", fieldValue);
                break;

            case NULL:
                invalidKeyList.add(fieldName);
                property.put("type", "null");
                property.put("value", fieldValue);
                break;
            default:
                break;
        }
        return property;
    }

    public static String getJsonSchema(String jsonDocument) throws IllegalArgumentException, IOException {
        Map<String, Object> map = OBJECT_MAPPER.readValue(jsonDocument, new TypeReference<Map<String, Object>>() {
        });
        return getJsonSchema(map);
    }

    public static String getJsonSchema(Map<String, Object> jsonDocument) throws IllegalArgumentException, IOException {

        JsonNode properties = createProperty(OBJECT_MAPPER.convertValue(jsonDocument, JsonNode.class));
        return getJsonSchema(properties);

    }

    public static JSONObject getSampleJson(String json) {
        JSONObject metaProperties = new JSONObject(json).getJSONObject("properties");
        JSONObject obj = new JSONObject();
        for (String modelKey : metaProperties.keySet()) {
            JSONObject modelObject = metaProperties.getJSONObject(modelKey);
            obj.put(modelKey, getObjectFromNewSchema(modelObject, modelKey, new JSONObject()));
        }
        return obj;
    }

    private static Object getObjectFromNewSchema(JSONObject modelObject, String parentKey, JSONObject obj) {
        JSONObject properties = null;
        if (modelObject != null) {
            if (modelObject.getString("type").equals("array")) {
                org.json.JSONArray array = new org.json.JSONArray();
                JSONObject items;
                try {
                    items = modelObject.getJSONObject("items");
                } catch (JSONException e) {
                    return modelObject.get("value");
                }
                JSONObject object = new JSONObject();
                for (String column : items.keySet()) {
                    try {
                        JSONObject columnObject = items.getJSONObject(column);
                        object.put(column, getObjectFromNewSchema(columnObject, column, obj));
                    } catch (JSONException e) {
                        if (column.equalsIgnoreCase("value") || column.equalsIgnoreCase("properties")) {
                            array.put(items.get(column));
                        }
                    }
                }
                if (object.keySet().size() > 0) {
                    array.put(object);
                }
                return array;
            } else if (modelObject.getString("type").equals("object")) {
                properties = modelObject.getJSONObject("properties");
                JSONObject object = new JSONObject();
                for (String column : properties.keySet()) {
                    JSONObject columnObject = properties.getJSONObject(column);
                    object.put(column, getObjectFromNewSchema(columnObject, column, obj));
                }
                return object;
            } else if (modelObject.get("type").equals("nullObject")) {
                return modelObject.get("properties");
            } else {
                return modelObject.get("value");
            }
        }
        return obj;
    }

    private static Set<String> list = new HashSet<>();

    private static Map<String,String>  getNullValueKeyPath(String schemajson) {
        JSONObject metaProperties = new JSONObject(schemajson).getJSONObject("properties");
        for(String modelKey : metaProperties.keySet()) {
            JSONObject modelObject = metaProperties.getJSONObject(modelKey);
            getKeyPathFromNewSchema(modelObject, modelKey, null);
        }
        Map<String,String> invalidKeyPath = new HashMap<>();
        for(String s : invalidKeyList){
            for(String str : list){
                if(str.contains(s)){
                    invalidKeyPath.put(s,str);
                }
            }
        }
        return invalidKeyPath;
    }

    private static void getKeyPathFromNewSchema(JSONObject modelObject, String parentKey,
                                                String currentKey) {
        JSONObject properties = null;
        if (modelObject != null) {
            if (modelObject.getString("type").equals("array")) {
                JSONObject items;
                try {
                    items = modelObject.getJSONObject("items");
                } catch (JSONException e) {
                    return;
                }
                for (String column : items.keySet()) {
                    try {
                        JSONObject columnObject = items.getJSONObject(column);
                        list.add(parentKey+"."+column);
                        getKeyPathFromNewSchema(columnObject, parentKey + "." + column, column);
                    } catch (JSONException e) {
                        // continue to get more key
                    }
                }
            } else if (modelObject.getString("type").equals("object")) {
                properties = modelObject.getJSONObject("properties");
                parentKey = parentKey != null ? parentKey + "." : "";
                for (String column : properties.keySet()) {
                    JSONObject columnObject = properties.getJSONObject(column);
                    list.add(parentKey + column);
                    getKeyPathFromNewSchema(columnObject, parentKey + column, column);
                }
            }
        }
    }

}
