package org.pih.hivmigration.etl.sql

import org.apache.commons.dbutils.handlers.MapListHandler

import java.util.stream.Collectors;

class TableStager extends SqlMigrator {

    String oracleTableName
    String mysqlTableName

    TableStager(String oracleTableName) {
        this.oracleTableName = oracleTableName
        this.mysqlTableName = "hivmigration_" + oracleTableName.replace("HIV_", "").toLowerCase()
    }
    @Override
    void migrate() {
        def datatypeMap = [VARCHAR2: "TEXT", NUMBER: "NUMERIC"]

        List<Map<String, Object>> oracleSchema = selectOracle(
                "select COLUMN_NAME, DATA_TYPE, DATA_LENGTH from ALL_TAB_COLUMNS where TABLE_NAME='" + oracleTableName + "'",
                new MapListHandler())


        List<Map.Entry<String, String>> mysqlTableInfo = oracleSchema.stream()
            .map({ row ->             
                new AbstractMap.SimpleEntry<>(
                        row.get("COLUMN_NAME").toString().toLowerCase(),
                        datatypeMap[row.get("DATA_TYPE")] ?: ((row.get("DATA_TYPE") == "CHAR") && row.get("DATA_LENGTH") ? (row.get("DATA_TYPE") + "(" + row.get("DATA_LENGTH") + ")") : (row.get("DATA_TYPE"))))
            })
            .collect(Collectors.toList())

        List<String> columnNames = mysqlTableInfo.stream()
                .map(e -> e.getKey())
                .collect(Collectors.toList())

        String mysqlSchema = mysqlTableInfo.stream()
                .map(col -> col.getKey() + " " + col.getValue())
                .collect(Collectors.joining((",\n")))

        executeMysql("Creating table " + mysqlTableName + " based on schema from " + oracleTableName,
                "CREATE TABLE " + mysqlTableName + " ( " + mysqlSchema + " );")

        // Prepare the load statements
        String loadInsert = ("INSERT INTO " + mysqlTableName + " ( " + columnNames.join(", ") + " ) VALUES ( "
            + columnNames.stream().map(n -> "?").collect(Collectors.joining(", "))
            + " ) ")
        String loadSelect = "select t." + columnNames.join(", t.") + " from " + oracleTableName + " t "
        // Filter out test data if there is a known column to join on
        if (columnNames.contains("patient_id")) {
            loadSelect += " , HIV_DEMOGRAPHICS_REAL d where d.patient_id = t.patient_id"
        } else if (columnNames.contains("encounter_id")) {
            loadSelect += " , HIV_ENCOUNTERS e, HIV_DEMOGRAPHICS_REAL d where t.encounter_id = e.encounter_id and e.patient_id = d.patient_id"
        }
        loadFromOracleToMySql(loadInsert, loadSelect)
    }

    @Override
    void revert() {
        executeMysql("DROP TABLE IF EXISTS " + mysqlTableName)
    }
}
