package org.veupathdb.eda.binaryfiles.printer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.veupathdb.service.eda.ss.model.variable.Utf8EncodingLengthProperties;
import org.veupathdb.service.eda.ss.model.variable.binary.*;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

/*
 * Print to tab delimited text the contents of binary files produced by the eda binary file dumper.
 */
public class BinaryFilePrinter {
  private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
  private static final int RECORDS_PER_BUFFER = 100;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void printBinaryFile(Path binaryFile, Path metajsonFile) {
    BinaryFilesManager.Metadata metadata = readMetaJsonFile(metajsonFile);
    String binaryFileNm = binaryFile.toFile().getName();
    
    switch (binaryFileNm) {
    case BinaryFilesManager.ANCESTORS_FILE_NAME:
      printAncestorFile(binaryFile, metadata);
      break;
    case BinaryFilesManager.IDS_MAP_FILE_NAME:
      printIdsMapFile(binaryFile, metadata);
      break;
    default:
      try {
        printVarFile(binaryFile, metadata);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
  
  private static BinaryFilesManager.Metadata readMetaJsonFile(Path metajsonFile) {
    if (!metajsonFile.toFile().exists()) throw new RuntimeException("metajson file '" + metajsonFile + "' does not exist");
    try {
      return OBJECT_MAPPER.readValue(metajsonFile.toFile(), BinaryFilesManager.Metadata.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed reading meta json file", e);
    }
  }
  
  private static VariableType getVarType(BinaryFilesManager.VariableMeta meta) {
    return VariableType.fromString(meta.getType());
  }

  private static void printAncestorFile(Path binaryFile, BinaryFilesManager.Metadata metadata) {
    int numAncestors = metadata.getBytesReservedPerAncestor().size();

    ListConverter<Long> ancestorConverter = new ListConverter<>(new LongValueConverter(), numAncestors + 1);

    try (DualBufferBinaryRecordReader<List<Long>> ancestorReader = new DualBufferBinaryRecordReader<>(
        binaryFile,
        ancestorConverter.numBytes(),
        RECORDS_PER_BUFFER,
        ancestorConverter::fromBytes,
        THREAD_POOL,
        THREAD_POOL)) {

        while (true) {
          if (!ancestorReader.hasNext()) {
            break;
          }
          List<Long> ancestorRow = ancestorReader.next();

          String text = ancestorRow.stream()
            .map(n -> String.valueOf(n))
            .collect(Collectors.joining("\t"));
        
          System.out.println(text);
        }

      } catch (IOException e) {
      throw new RuntimeException("Failed attempting to read file " + binaryFile, e);
    }
  }
  
  private static void printIdsMapFile(Path binaryFile, BinaryFilesManager.Metadata metadata) {
    List<Integer> bytesReservedPerAncestors = metadata.getBytesReservedPerAncestor();
    int bytesReservedForId = metadata.getBytesReservedForId();

    RecordIdValuesConverter converter = new RecordIdValuesConverter(bytesReservedPerAncestors, bytesReservedForId);
    
    try (DualBufferBinaryRecordReader<RecordIdValues> reader =
        new DualBufferBinaryRecordReader<>(binaryFile, converter.numBytes(), RECORDS_PER_BUFFER, converter::fromBytes,
                THREAD_POOL, THREAD_POOL)){

        while (true) {
          if (!reader.hasNext())
            break;

          RecordIdValues idsMapRow = reader.next();
          List<String> rowStrings = new ArrayList<>();
          rowStrings.add(Long.toString(idsMapRow.getIdIndex()));
          rowStrings.add(idsMapRow.getEntityId());
          rowStrings.addAll(idsMapRow.getAncestorIds());
        
          System.out.println(String.join("\t", rowStrings));
        }

      } catch (IOException e) {
      throw new RuntimeException("Failed attempting to read file " + binaryFile, e);
    }
  }    
  
  private static void printVarFile(Path binaryFile, BinaryFilesManager.Metadata metadata) throws Exception {
    BinaryFilesManager.VariableMeta variableMeta = metadata.getVariableMetadata().stream()
        .filter(var -> binaryFile.getFileName().toString().contains(var.getVariableId()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Cannot find variable metadata, unable to print variable."));

    BinaryConverter<?>converter;
    if (binaryFile.getFileName().toString().contains("utf8")) {
      if (getVarType(variableMeta) == VariableType.DATE) {
        converter = new StringValueConverter(24);
      } else if (variableMeta.getProperties() instanceof Utf8EncodingLengthProperties) {
        converter = new StringValueConverter(((Utf8EncodingLengthProperties) variableMeta.getProperties()).getMaxLength());
      } else {
        // Integers don't have encoding information.
        converter = new StringValueConverter(5);
      }
    } else {
      converter = getVarType(variableMeta).getConverterSupplier().apply(variableMeta.getProperties());
    }

    ValueWithIdDeserializer<?> varDeserializer = new ValueWithIdDeserializer<>(converter);
    
    try (DualBufferBinaryRecordReader<VariableValueIdPair<?>> varReader = new DualBufferBinaryRecordReader<>(binaryFile,
        varDeserializer.numBytes(), RECORDS_PER_BUFFER, varDeserializer::fromBytes, THREAD_POOL, THREAD_POOL)){

        while (true) {
          if (!varReader.hasNext())
            break;
          VariableValueIdPair<?> varRow = varReader.next();

          // This is a hack to ensure byte arrays get properly encoded before printed.
          // Calling toString on a byte array will otherwise return address in memory.
          String strValue = varRow.getValue() instanceof byte[]
              ? new String((byte[]) varRow.getValue(), StandardCharsets.UTF_8)
              : varRow.getValue().toString();

          String text = varRow.getIdIndex() + "\t" + strValue;
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

