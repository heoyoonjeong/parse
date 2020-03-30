package com.example.demo;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Component
public class FileReader {

    public List<String> readCsvFile(String filePath, char delimiter, String characterSet) throws IOException {

        File csvFile = new File(filePath);
        Reader in = new java.io.FileReader(filePath);
        CSVParser parser = CSVFormat.DEFAULT.withDelimiter(delimiter).parse(in);
        CSVRecord csvRecord = parser.getRecords().get(0);
        Iterator<String> headerIt = csvRecord.iterator();
        List<String> headerList = new ArrayList<>();
        while (headerIt.hasNext()){
            headerList.add(headerIt.next());
        }
        return headerList;
    }


}
