package org.pih.hivmigration.etl;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 * User defined functions and related utility methods
 */
public class Functions {

    public static String generateUuid(Object tableName, Object primaryKey) {
        try {
            String namespaceId = "53a5b5f4-3239-4a27-8674-5c3bb8116692";
            String namespaceIdAndName = namespaceId + "-" + tableName + "-" + primaryKey;
            byte[] bytes = namespaceIdAndName.getBytes("UTF-8");
            UUID uuid = UUID.nameUUIDFromBytes(bytes);
            return uuid.toString();
        }
        catch (UnsupportedEncodingException uee) {
            throw new RuntimeException("Unable to generate uuid due to unsupported UTF-8", uee);
        }
    }

    public static class Uuid implements UDF2<Object, Object, String> {
        @Override
        public String call(Object table, Object primaryKey) {
            return Functions.generateUuid(table, primaryKey);
        }
    }

    public static class SubstringBefore implements UDF2<String, String, String> {
        @Override
        public String call(String columnValue, String separator) {
            return StringUtils.substringBefore(columnValue, separator);
        }
    }
}
