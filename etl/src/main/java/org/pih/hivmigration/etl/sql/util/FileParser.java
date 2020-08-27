package org.pih.hivmigration.etl.sql.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileParser {

    // TODO confirm that this works with CSVs with more than one column (if we ever need it)
    public static List<List<String>> loadCSV(String fileName)  {

        List<List<String>> rows = new ArrayList<>();

        File file = new File(FileParser.class.getClassLoader().getResource(fileName).getFile());

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                List<String> row = Arrays.asList(line.split(","));
                rows.add(row);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to read file: " + fileName);
        }

        return rows;
    }
}
