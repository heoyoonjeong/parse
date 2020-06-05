package com.example.demo;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class FileReaderTest {

    private final String dirPath = "D:\\P2_FULL";
    private final String filePath = "D:\\workhour\\test.csv";
    @Autowired
    FileReader reader;



    @Test
    void restTest() throws Exception {
      FlatFileItemReader<Object> reader = new FlatFileItemReader<>();
      reader.setResource(new FileSystemResource(filePath));


    }

    @Test
    void readCsvFile() throws IOException {
        FileReader fileReader = new FileReader();
        fileReader.readCsvFile("C:\\Users\\hyj\\workday\\ftp\\outbound\\INT-HR-001\\INT-HR-001_Personal_Data_20200317-00.csv", '^',"UTF8");
    }

    @Test
    void printCsvHeader() throws IOException{
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

              System.out.println(column);
            }

            System.out.println();

          }
        }
      }

    }



  @Test
  void printCsvHeaderCoumns() throws IOException{
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
            System.out.print(columnBuilder.toString());
          }

          System.out.println();

        }
      }
    }

  }

  @Test
  void printCsvHeaderCoumnsStatement() throws IOException{
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
          String tableName = StringUtils.substringBeforeLast(fileName, "_");
          tableName = StringUtils.substringAfterLast(tableName,"-");

          System.out.print("\"INSERT INTO ");
          System.out.print(tableName);
          System.out.print(" ( UUID, File_Path, ");


          for (int i = 0; i < list.size() -1; i++) {
            String column = list.get(i);

            column = StringUtils.trim(column);
            while (column.matches("\\S+\\s+.+")) {
              column = column.replaceFirst("\\s", "_");
            }
            System.out.print(column);
            System.out.print(",");
          }
          System.out.print(list.get(list.size() -1));


          System.out.print(" ) VALUES ( ?, ?,");
          for(int i = 0; i < list.size() -1; i++){
            System.out.print(" ?, ");
          }
          System.out.print(" ?");
          System.out.print(")\",");
          System.out.print("\"UUID\", \"FIle_Path\", ");
          for (int i = 0; i < list.size()-1; i++) {
            String column = list.get(i);
            StringBuilder columnBuilder = new StringBuilder();
            columnBuilder.append("\"");
            columnBuilder.append(column);
            columnBuilder.append("\",");
            System.out.print(columnBuilder.toString());
          }
          String lastcolumn = list.get(list.size() -1);
          StringBuilder columnBuilderlast = new StringBuilder();
          columnBuilderlast.append("\"");
          columnBuilderlast.append(lastcolumn);
          columnBuilderlast.append("\"");
          System.out.print(columnBuilderlast.toString());
          System.out.println();

        }
      }
    }

  }

  @Test
  void printTableName() throws IOException{
    File dir = new File(dirPath);
    for (File subDir : dir.listFiles()) {
      if (subDir.isDirectory()) {
        String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
        for (File conversionData : subDir.listFiles()) {
          String fileName = conversionData.getName();
          String tableName = StringUtils.substringBeforeLast(fileName, "_");
         // tableName = StringUtils.substringAfterLast(tableName,"-");

            System.out.println(tableName);
          }
        }
      }
    }

  @Test
  void printTruncateTableName() throws IOException{
    File dir = new File(dirPath);
    for (File subDir : dir.listFiles()) {
      if (subDir.isDirectory()) {
        String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
        for (File conversionData : subDir.listFiles()) {

          String fileName = conversionData.getName();
          if(StringUtils.substringAfterLast(fileName,".").equals("csv") && StringUtils.substringAfterLast(fileName,"_").equals("20200601-00.csv")){
          String tableName = StringUtils.substringBeforeLast(fileName, "_");
          tableName = StringUtils.substringAfterLast(tableName,"-");

          System.out.println("TRUNCATE TABLE " +tableName+ ";");
        }
      }}
    }
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
                            field = field.replaceFirst("\\s\\S", changeChar.toUpperCase());
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
                            field = field.replaceFirst("\\s\\S", changeChar.toUpperCase());
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
                            field = field.replaceFirst("\\s\\S", changeChar.toUpperCase());
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
                    lastQuery += "_The_Latest_Snapshot` ( \n" +
                        "\t`UUID` VARCHAR(100) NOT NULL COLLATE 'utf8mb4_unicode_ci',\n" +
                        "\t`FIle_Name` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_unicode_ci',\n" +
                        "\t`Last_Updated` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"+
                        "\tPRIMARY KEY ( )\n" +
                        ")\n";


                    lastQuery += "COLLATE='utf8mb4_unicode_ci' ENGINE=InnoDB;";
                    System.out.println(lastQuery);
                  System.out.println();
                }
            }
        }

    }
  @Test
  void alterColumnTable() throws IOException {
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
/*          String lastQuery = "ALTER TABLE ";
          lastQuery += tableName;
          lastQuery += " ADD Last_Updated DATETIME NULL DEFAULT CURRENT_TIMESTAMP";
          System.out.println(lastQuery);*/
          String lastQuery2 = "ALTER TABLE ";
          lastQuery2 += tableName;
          lastQuery2 += " ADD UUID VARCHAR(100) FIRST ;";
          System.out.println(lastQuery2);

        }
      }
    }

  }


  @Test
  void alterSTATUSColumnTable() throws IOException {
    File dir = new File(dirPath);
    List<String> tableList = new ArrayList<>();
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
          if(tableList.contains(tableName)){
            continue;
          }
          tableList.add(tableName);
          String lastQuery2 = "ALTER TABLE ";
          lastQuery2 += tableName;
          lastQuery2 += " ADD CONSTRAINT " + tableName + "_pk PRIMARY KEY (File_Path, ";
          System.out.println(lastQuery2);

        }
      }
    }

  }

  @Test
  public void printFileName() throws IOException {
    File dir = new File(dirPath);
    for (File subDir : dir.listFiles()) {
      if (subDir.isDirectory()) {
        String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
        for (File conversionData : subDir.listFiles()) {
          String fileFullName = conversionData.getName();
          String fileName = StringUtils.substringBeforeLast(fileFullName ,".");
          String fileType = StringUtils.substringAfterLast(fileFullName ,".");
          if(!fileType.equals("csv")) continue;
          String dataType = StringUtils.substringBeforeLast(fileFullName,"_");
          String timeZone = StringUtils.substringAfterLast(dataType,"_");
          String seq = StringUtils.substringAfterLast(fileName, "-");
          Integer seqLen  = seq.length();
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append("(\"");
          stringBuilder.append(dataType);
          stringBuilder.append("\", \"");
          stringBuilder.append(fileType);
          stringBuilder.append("\", ");
          if(timeZone.equals("KST") || timeZone.equals("GMT") || timeZone.equals("PST")){
            stringBuilder.append("\"");
            stringBuilder.append(timeZone);
            stringBuilder.append("\", ");
          }else {
            stringBuilder.append("null");
            stringBuilder.append(", ");
          }
          stringBuilder.append("99, ");
          stringBuilder.append(seqLen);
          stringBuilder.append(", now(), now()),");

          System.out.println(stringBuilder);

        }
      }
    }

  }

  @Test
  public void testDateTime(){
    int len = 10;
    String formatter = "%0" + len + "d";
    System.out.println(String.format(formatter, 20));
  }


  @Test
  void selectSql() throws IOException {
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
          String lastQuery = "        <when test='tableName.equals(\"";
          lastQuery += tableName.toLowerCase();
          lastQuery += "\")'>\n" +
              "          <include refid=\"";
          String pkset = tableName.toLowerCase() + "_pk_set";
          lastQuery += pkset;
          lastQuery += "\" />\n" +
              "        </when>";

          System.out.println(lastQuery);
        }
      }
    }

  }

  @Test
  void selectpksetSql() throws IOException {
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
          String lastQuery = "   <sql id=\"";
          String pkset = tableName.toLowerCase() + "_pk_set_insert";
          lastQuery += pkset;
          lastQuery += "\">\n" +
              "  </sql>";

          System.out.println(lastQuery);
        }
      }
    }
}


  @Test
  void selectEnumSql() throws IOException {
    File dir = new File(dirPath);
    for (File subDir : dir.listFiles()) {
      if (subDir.isDirectory()) {
        String subDirPath = dirPath + "\\" + subDir.getName() + "\\";
        for (File conversionData : subDir.listFiles()) {
          String fileName = conversionData.getName();
          String tableName = StringUtils.substringBeforeLast(fileName, "_");
          tableName = StringUtils.substringAfterLast(tableName, "-");
          String dataType = StringUtils.substringBeforeLast(fileName, "_");
          String enumName = StringUtils.substringAfter(tableName, "_").toUpperCase();
          if (StringUtils.substringAfterLast(fileName, ".").equals("csv")) {
            // parser.testParse("INT-HR-001_Personal_Data_20200317-000.csv");
            String lastQuery = enumName;
            lastQuery += "(\"";
            lastQuery += dataType;
            lastQuery += "\", \"";
            lastQuery += tableName.toLowerCase();
            lastQuery += "\", \"D:\\\\outbound\\\\";
            lastQuery += StringUtils.substringBefore(fileName, "_");
            lastQuery += "\\\\\"),";
            System.out.println(lastQuery);
          }
        }
      }
    }

  }



  @Test
  void insertIntoDataTypeInprogress () throws IOException {
    File dir = new File(dirPath);
    for (File subDir : dir.listFiles()) {
      if (subDir.isDirectory()) {
        String subDirPath = dirPath + "\\";
        for (File conversionData : subDir.listFiles()) {
          String fileName = conversionData.getName();
          String tableName = StringUtils.substringBeforeLast(fileName, "_");
          tableName = StringUtils.substringAfterLast(tableName, "-");
          String dataType = StringUtils.substringBeforeLast(fileName, "_");
          String enumName = StringUtils.substringAfter(tableName, "_").toUpperCase();
          if (StringUtils.substringAfterLast(fileName, ".").equals("csv") && StringUtils.substringAfterLast(fileName, "_").equals("20200601-00.csv")) {
            String lastQuery = "";
            lastQuery += "('";
            lastQuery += dataType;
            lastQuery += "', 0, '20200601', '";
            lastQuery += fileName;
            lastQuery += "', 'WAIT', '";
            lastQuery += conversionData.getAbsolutePath();
            lastQuery += "', 'N'),";

            System.out.println(lastQuery);
          }
          // parser.testParse("INT-HR-001_Personal_Data_20200317-000.csv");

        }
      }
    }
  }

    @Test
    void deleteFromNotInSnapshot () throws IOException {
      File dir = new File(dirPath);
      for (File conversionData : dir.listFiles()) {

            String fileName = conversionData.getName();
            String tableName = StringUtils.substringBeforeLast(fileName, "_");
            tableName = StringUtils.substringAfterLast(tableName,"-");
            String dataType = StringUtils.substringBeforeLast(fileName,"_");
            String enumName = StringUtils.substringAfter(tableName,"_").toUpperCase();

              String lastQuery = "";
              lastQuery += "('";
              lastQuery += dataType;
              lastQuery += "', 0, '20200602', '";
              lastQuery += fileName;
              lastQuery += "', 'WAIT', '";
              lastQuery += conversionData.getAbsolutePath();
              lastQuery += "', 'N'),";

              System.out.println(lastQuery);

            // parser.testParse("INT-HR-001_Personal_Data_20200317-000.csv");

          }
        }



  @Test
  void realDeleteFromNotInSnapshot () throws IOException {
    File dir = new File(dirPath);
    for (File conversionData : dir.listFiles()) {

      String fileName = conversionData.getName();
      String tableName = StringUtils.substringBeforeLast(fileName, "_");
      tableName = StringUtils.substringAfterLast(tableName,"-");
      String dataType = StringUtils.substringBeforeLast(fileName,"_");
      String enumName = StringUtils.substringAfter(tableName,"_").toUpperCase();
      if(StringUtils.substringAfterLast(tableName, "_").equals("GMT") ||StringUtils.substringAfterLast(tableName, "_").equals("KST") ||StringUtils.substringAfterLast(tableName, "_").equals("PST")  ){
        tableName = StringUtils.substringBeforeLast(tableName, "_");
      }

      String lastQuery = "";
      lastQuery += "select * from ";
      lastQuery += tableName;
      lastQuery += " where uuid not in ( select uuid from ";
      lastQuery += tableName;
      lastQuery += "_the_latest_snapshot);";


      System.out.println(lastQuery);

      // parser.testParse("INT-HR-001_Personal_Data_20200317-000.csv");

    }
  }



}
