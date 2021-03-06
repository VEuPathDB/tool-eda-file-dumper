package org.veupathdb.eda.binaryfiles.printer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.json.JSONException;
import org.json.JSONObject;
import org.veupathdb.eda.binaryfiles.BinaryFilesManager;
import org.veupathdb.eda.binaryfiles.dumper.FilesDumper;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdDeserializer;

/*
 * Print to tab delimited text the contents of binary files produced by the eda binary file dumper.
 */
public class BinaryFilePrinter {
  
  private static final int RECORDS_PER_BUFFER = 100;

  public static void printBinaryFile(Path binaryFile, Path metajsonFile) {
    
    JSONObject metajson = readMetaJsonFile(metajsonFile);
    String binaryFileNm = binaryFile.toFile().getName();

    if (binaryFileNm.equals(BinaryFilesManager.ANCESTORS_FILE_NAME)) 
      printAncestorFile(binaryFile, metajson);
    
    else {
      BinaryConverter<?> converter = new StringValueConverter(FilesDumper.BYTES_RESERVED_FOR_ID_STRING); // assume converter for id_map file
      if (!binaryFileNm.equals(BinaryFilesManager.IDS_MAP_FILE_NAME)) 
        converter = getVarType(binaryFileNm, metajson).getConverterSupplier().get();     
 
      printVarFile(binaryFile, converter);
    }
  }
  
  private static JSONObject readMetaJsonFile(Path metajsonFile) {
    if (!metajsonFile.toFile().exists()) throw new RuntimeException("metajson file '" + metajsonFile + "' does not exist");
    try {
      String jsonString = Files.readString(metajsonFile);
      JSONObject json = new JSONObject(jsonString);
      return json;
    } catch (IOException | JSONException e) {
      throw new RuntimeException("Failed reading meta json file", e);
    }
  }
  
  private static VariableType getVarType(String fileBaseName, JSONObject metajson) {
    if (!metajson.has(fileBaseName)) throw new RuntimeException("Meta json file does not contain key: " + fileBaseName);
    return VariableType.fromString(metajson.getString(fileBaseName));
  }
  
  private static void printAncestorFile(Path binaryFile, JSONObject metajson) {

    int numAncestors = metajson.getInt(BinaryFilesManager.META_KEY_NUM_ANCESTORS);

    ListConverter<Long> ancestorConverter = new ListConverter<>(new LongValueConverter(), numAncestors + 1);
    
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
  
  private static void printVarFile(Path binaryFile, BinaryConverter<?> converter) {

    ValueWithIdDeserializer<?> varDeserializer = new ValueWithIdDeserializer<>(converter);
    
    try (DualBufferBinaryRecordReader varReader = new DualBufferBinaryRecordReader(binaryFile,
        varDeserializer.numBytes(), RECORDS_PER_BUFFER)){

        while (true) {
          Optional<byte[]> varRowBytes = varReader.next();
          if (varRowBytes.isEmpty())
            break;
          VariableValueIdPair<?> varRow = varRowBytes.map(varDeserializer::fromBytes)
            .orElseThrow(() -> new RuntimeException("Unexpected end of variables file"));

          String text = varRow.getIdIndex().toString() + "\t" + varRow.getValue().toString();        
          System.out.println(text);
        }

      } catch (IOException e) {
      throw new RuntimeException("Failed attempting to read file " + binaryFile.toString(), e);
    }

  }
    
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("USAGE: binaryFileToText <binaryFile> <metajsonFile>");
      System.exit(1);
    }

    Path binaryFilePath = Paths.get(args[0]);
    Path metajsonPath = Paths.get(args[1]);

    if (!Files.exists(binaryFilePath)) {
      throw new IllegalArgumentException(binaryFilePath.toAbsolutePath() + " does not exist.");
    }
    if (!Files.exists(metajsonPath)) {
      throw new IllegalArgumentException(metajsonPath.toAbsolutePath() + " does not exist.");
    }

    BinaryFilePrinter.printBinaryFile(binaryFilePath, metajsonPath);
  }
  
}

