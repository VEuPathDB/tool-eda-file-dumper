package org.veupathdb.eda.binaryfiles.printer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.veupathdb.eda.binaryfiles.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;

public class BinaryFilePrinter {
  
  private static final int RECORDS_PER_BUFFER = 100;

  public static void printBinaryFile(Path binaryFile, File metajsonFile) {
    
    JSONObject metajson = readMetaJsonFile(metajsonFile);
  
    if (binaryFile.getFileName().equals(BinaryFilesManager.ANCESTORS_FILE_NAME)) printAncestorFile(binaryFile, metajson);
    else {
      VariableType type = VariableType.STRING;  // assume idMap file
      if (!binaryFile.getFileName().equals(BinaryFilesManager.IDS_MAP_FILE_NAME))
        type = getVarType(binaryFile.toFile().getName(), metajson);
      printVarFile(binaryFile, type);
    }
  }
  
  private static JSONObject readMetaJsonFile(File metajsonFile) {
    if (!metajsonFile.exists()) throw new RuntimeException("metajson file '" + metajsonFile + "' does not exist");
    JSONParser parser = new JSONParser();
    try {
      return (JSONObject) parser.parse(new FileReader(metajsonFile));
    } catch (IOException | ParseException e) {
      throw new RuntimeException("Failed reading meta json file", e);
    }
  }
  
  private static VariableType getVarType(String fileName, JSONObject metajson) {
    return null;
  }
  
  private static void printAncestorFile(Path binaryFile, JSONObject metajson) {
    
    int numAncestors = metajson.getInt(BinaryFilesManager.META_KEY_NUM_ANCESTORS);
    ListConverter<Long> ancestorConverter = new ListConverter<>(new LongValueConverter(), numAncestors);
    
    try (DualBufferBinaryRecordReader ancestorReader = new DualBufferBinaryRecordReader(binaryFile,
        ancestorConverter.numBytes(), RECORDS_PER_BUFFER)){

      while (true) {
        Optional<byte[]> ancestorRowBytes = ancestorReader.next();
        if (ancestorRowBytes.isEmpty())
          break;
        List<Long> ancestorRow = ancestorRowBytes.map(ancestorConverter::fromBytes)
            .orElseThrow(() -> new RuntimeException("Unexpected end of ancestors file"));

        String text = ancestorRow.stream()
            .map(n -> String.valueOf(n))
            .collect(Collectors.joining("\t"));
        
        System.out.println(text);
      }

    } catch (IOException e) {
      throw new RuntimeException("Failed attempting to read file " + binaryFile.toString(), e);
    }
  }    
  
  private static void printVarFile(Path binaryFile, VariableType type) {
  }
  
}

