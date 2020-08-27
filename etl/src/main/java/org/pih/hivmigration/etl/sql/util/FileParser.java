package org.pih.hivmigration.etl.sql.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileParser {

    // TODO confirm that this works with CSVs with more than one column (if we ever need it)
    public static List<List<String>> loadCSV(String fileName)  {

        List<List<String>> rows = new ArrayList<>();

        try {
            InputStream is = FileParser.class.getResourceAsStream(fileName);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                List<String> row = Arrays.asList(line.split(","));
                rows.add(row);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to read file: " + fileName, e);
        }

        return rows;
    }
}
