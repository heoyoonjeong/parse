package com.example.demo;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class FileReaderTest {

    private final String dirPath = "D:\\workday\\outbound";
    @Autowired
    FileReader reader;

    @Test
    void readCsvFile() throws IOException {
        FileReader fileReader = new FileReader();
        fileReader.readCsvFile("C:\\Users\\hyj\\workday\\ftp\\outbound\\INT-HR-001\\INT-HR-001_Personal_Data_20200317-00.csv", '^',"UTF8");
    }

    @Test
    public void createFieldNameSet() throws IOException {
        File dir = new File(dirPath);
        for (File subDir : dir.listFiles()) {
            if (subDir.isDirectory()) {
                String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
                for (File conversionData : subDir.listFiles()) {
                    String fileName = conversionData.getName();
                    if(!StringUtils.substringAfterLast(fileName,".").equals("csv")){
                        continue;
                    }
                    List<String> list = reader.readCsvFile(subDirPath + fileName, '^', "UTF-8");
                    List<String> createQuery = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {
                        String column = list.get(i);

                        StringBuilder columnBuilder = new StringBuilder();
                        columnBuilder.append("\"");
                        columnBuilder.append(column);
                        columnBuilder.append("\",");
                        System.out.println(columnBuilder.toString());
                        createQuery.add(columnBuilder.toString());
                    }

                    System.out.println();

                }
            }
        }
    }


    @Test
    public void createQueryParam() throws IOException {

        File dir = new File(dirPath);
        for (File subDir : dir.listFiles()) {
            if (subDir.isDirectory()) {
                String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
                for (File conversionData : subDir.listFiles()) {
                    String fileName = conversionData.getName();
                    String fileType = StringUtils.substringAfterLast(fileName ,".");
                    if(!fileType.equals("csv")){
                      continue;
                    }
                    List<String> list = reader.readCsvFile(subDirPath + fileName, '^', "UTF-8");
                    List<String> createQuery = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {

                        String field = list.get(i);
                        field = field.toLowerCase();
                        while (field.contains("_")) {
                            int idx = field.indexOf("_");
                            String changeChar = field.substring(idx + 1, idx + 2);
                            field = field.replaceFirst("_\\D", changeChar.toUpperCase());
                        }
                        while (field.matches("\\S+\\s+.+")) {
                            int idx = field.indexOf(" ");
                            String changeChar = field.substring(idx + 1, idx + 2);
                            field = field.replaceFirst("\\s\\D", changeChar.toUpperCase());
                        }
                        StringBuilder columnBuilder = new StringBuilder();
                        columnBuilder.append("#{");
                        columnBuilder.append(field);
                        columnBuilder.append("},");
                        System.out.println(columnBuilder.toString());
                        createQuery.add(columnBuilder.toString());
                    }

                    System.out.println();

                }
            }
        }

    }


    @Test
    public void createFieldMapper() throws IOException {
        File dir = new File(dirPath);
        for (File subDir : dir.listFiles()) {
            if (subDir.isDirectory()) {
                String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
                for (File conversionData : subDir.listFiles()) {
                    String fileName = conversionData.getName();
                    if(!StringUtils.substringAfterLast(fileName,".").equals("csv")){
                        continue;
                    }
                    List<String> list = reader.readCsvFile(subDirPath + fileName, '^', "UTF-8");
                    List<String> createQuery = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {
                        String column = list.get(i);
                        String field = column.toLowerCase();
                        while (field.contains("_")) {
                            int idx = field.indexOf("_");
                            String changeChar = field.substring(idx + 1, idx + 2);
                            field = field.replaceFirst("_\\D", changeChar.toUpperCase());
                        }
                        while (field.matches("\\S+\\s+.+")) {
                            int idx = field.indexOf(" ");
                            String changeChar = field.substring(idx + 1, idx + 2);
                            field = field.replaceFirst("\\s\\D", changeChar.toUpperCase());
                        }
                        StringBuilder columnBuilder = new StringBuilder();
                        columnBuilder.append(".");
                        columnBuilder.append(field);
                        columnBuilder.append("(fieldSet.readString(\"");
                        columnBuilder.append(column);
                        columnBuilder.append("\"))");
                        System.out.println(columnBuilder.toString());
                        createQuery.add(columnBuilder.toString());
                    }

                    System.out.println();

                }
            }
        }
    }

    @Test
    public void createDTO() throws IOException {
        File dir = new File(dirPath);
        for (File subDir : dir.listFiles()) {
            if (subDir.isDirectory()) {
                String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
                for (File conversionData : subDir.listFiles()) {
                    String fileName = conversionData.getName();
                  String fileType = StringUtils.substringAfterLast(fileName ,".");
                  if(!fileType.equals("csv")){
                    continue;
                  }
                    List<String> list = reader.readCsvFile(subDirPath + fileName, '^', "UTF-8");
                    List<String> createQuery = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {
                        String column = list.get(i);
                        String field = list.get(i);
                        field = field.toLowerCase();
                        while (field.contains("_")) {
                            int idx = field.indexOf("_");
                            String changeChar = field.substring(idx + 1, idx + 2);
                            field = field.replaceFirst("_\\D", changeChar.toUpperCase());
                        }
                        while (field.matches("\\S+\\s+.+")) {
                            int idx = field.indexOf(" ");
                            String changeChar = field.substring(idx + 1, idx + 2);
                            field = field.replaceFirst("\\s\\D", changeChar.toUpperCase());
                        }
                        StringBuilder columnBuilder = new StringBuilder();
                        columnBuilder.append("private String ");
                        columnBuilder.append(field);
                        columnBuilder.append(";");
                        System.out.println(columnBuilder.toString());
                        createQuery.add(columnBuilder.toString());
                    }

                    System.out.println();

                }
            }
        }

    }

    @Test
    public void regexTest(){
        String sampleString = "job detail";
        String sampleString1 = "job ";
        String sampleString2 = " job";
        System.out.println();
    }

    @Test
    void createTable() throws IOException {
        File dir = new File(dirPath);
        for (File subDir : dir.listFiles()) {
            if (subDir.isDirectory()) {
                String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
                for (File conversionData : subDir.listFiles()) {
                    String fileName = conversionData.getName();
                    String tableName = StringUtils.substringBeforeLast(fileName, "_");
                    tableName = StringUtils.substringAfterLast(tableName,"-");
                    if(!StringUtils.substringAfterLast(fileName,".").equals("csv")){
                        continue;
                    }
                    // parser.testParse("INT-HR-001_Personal_Data_20200317-000.csv");
                    String lastQuery = "CREATE TABLE `";
                    lastQuery += tableName;
                    lastQuery += "` (";

                    List<String> list = reader.readCsvFile(subDirPath + fileName, '^', "UTF-8");
                    List<String> createQuery = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {
                        String column = list.get(i);

                        column = StringUtils.trim(column);
                        while (column.matches("\\S+\\s+.+")) {
                            column = column.replaceFirst("\\s", "_");
                        }
                        StringBuilder columnBuilder = new StringBuilder();
                        columnBuilder.append("`");
                        columnBuilder.append(column);
                        columnBuilder.append("` VARCHAR(50) NOT NULL");
                        createQuery.add(columnBuilder.toString());
                    }
                    String createInner = createQuery.toString();
                    createInner = StringUtils.remove(createInner, "]");
                    createInner = StringUtils.remove(createInner, "[");
                    lastQuery += createInner;
                    lastQuery += ")COLLATE='utf8mb4_unicode_ci' ENGINE=InnoDB;";
                    System.out.println(lastQuery);
                }
            }
        }

    }
}
