package com.example.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;

@SpringBootTest
class ExcelReaderTest {

    private final String filePath = "D:\\org_load.csv";
  @Autowired
  FileReader reader;

  @Test
  public  void testSort() throws IOException {
    File csvFile = new File(filePath);
    Reader in = new java.io.FileReader(filePath);
    CSVParser parser = CSVFormat.DEFAULT.parse(in);
    List<CSVRecord> records = parser.getRecords();
    List<String> parentList = new ArrayList<>();
    parentList.add(records.get(0).get(1));
    parentList.add(records.get(1).get(1));



    records.forEach(
          record -> {
          if(!parentList.contains(record.get(1))){
            System.out.println(record.get(1));
            parentList.add(record.get(1));
          }else{
            parentList.add(record.get(0));
          }
        }
    );


    }

}
