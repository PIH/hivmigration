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
        def columnNameMap = [PATIENT_ID: "source_patient_id", ENCOUNTER_ID: "source_encounter_id"]
        def datatypeMap = [VARCHAR2: "TEXT", NUMBER: "NUMERIC"]

        List<Map<String, Object>> oracleSchema = selectOracle(
                "select COLUMN_NAME, DATA_TYPE, DATA_LENGTH from ALL_TAB_COLUMNS where TABLE_NAME='" + oracleTableName + "'",
                new MapListHandler())

        List<String> oracleColumnNames = oracleSchema.stream()
            .map( { row -> row.get("COLUMN_NAME").toString() })
            .collect(Collectors.toList())

        List<Map.Entry<String, String>> mysqlTableInfo = oracleSchema.stream()
            .map({ row ->
                new AbstractMap.SimpleEntry<>(
                        columnNameMap[row.get("COLUMN_NAME").toString()] ?: row.get("COLUMN_NAME").toString().toLowerCase(),
                        datatypeMap[row.get("DATA_TYPE")] ?: ((row.get("DATA_TYPE") == "CHAR") && row.get("DATA_LENGTH") ? (row.get("DATA_TYPE") + "(" + row.get("DATA_LENGTH") + ")") : (row.get("DATA_TYPE"))))
            })
            .collect(Collectors.toList())

        List<String> mysqlColumnNames = mysqlTableInfo.stream()
                .map(e -> e.getKey())
                .collect(Collectors.toList())

        String mysqlSchema = mysqlTableInfo.stream()
                .map(col -> col.getKey() + " " + col.getValue())
                .collect(Collectors.joining((",\n")))

        executeMysql("Creating table " + mysqlTableName + " based on schema from " + oracleTableName,
                "CREATE TABLE " + mysqlTableName + " ( " + mysqlSchema + " );")

        // Prepare the load statements
        String loadInsert = ("INSERT INTO " + mysqlTableName + " ( " + mysqlColumnNames.join(", ") + " ) VALUES ( "
            + mysqlColumnNames.stream().map(n -> "?").collect(Collectors.joining(", "))
            + " ) ")
        String loadSelect = "select t." + oracleColumnNames.join(", t.") + " from " + oracleTableName + " t "
        // Filter out test data if there is a known column to join on
        if (oracleColumnNames.contains("PATIENT_ID")) {
            loadSelect += " , HIV_DEMOGRAPHICS_REAL d where d.PATIENT_ID = t.PATIENT_ID"
        } else if (oracleColumnNames.contains("ENCOUNTER_ID")) {
            loadSelect += " , HIV_ENCOUNTERS e, HIV_DEMOGRAPHICS_REAL d where t.ENCOUNTER_ID = e.ENCOUNTER_ID and e.PATIENT_ID = d.PATIENT_ID"
        }
        loadFromOracleToMySql(loadInsert, loadSelect)
    }

    @Override
    void revert() {
        executeMysql("DROP TABLE IF EXISTS " + mysqlTableName)
    }
}
