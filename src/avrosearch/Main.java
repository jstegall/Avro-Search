package avrosearch;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class Main
{
  /**************************************************************************
  * Main entry into the application. Accepts the following arguments:
  *
  * 1. File - The AVRO file to deserialize and search.
  * 2. Key - The record header/key to find. Nested objects can be denoted
  *          using a period (".").
  * 3. Value - The value of the record header/key to find.
  **************************************************************************/
  public static void main(String[] args)
  {
    try
    {
      if (args.length != 3)
        throw new IllegalArgumentException("Usage: avro-search {file} {key} {value}");

      GenericRecord found = searchAvroFile(args[0], args[1], args[2]);

      if (found == null)
        System.out.println("No record found with field: '" + args[1] + "' matching '" + args[2] + "'");
      else
        writeJson(found);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }

  /****************************************************************************
  * Tests if the given field in an AVRO record is equal to the specified value.
  *
  * @param record
  *   The AVRO record.
  * @param key
  *   The record field/header to find.
  * @param value
  *   The value of the record field/header to find.
  * @return
  *   True if the record has a key and value that match the specified key/value
  *   pair; false otherwise.
  ****************************************************************************/
  private static boolean isMatch(final GenericRecord record, final String key, final String value)
  {
    if (record != null)
    {
      final Object recordValue = record.get(key);

      if (recordValue != null)
      {
        if (recordValue.toString().equals(value))
          return true;
      }
    }

    return false;
  }

  /****************************************************************************
  * Opens an AVRO file and searches it for the specified key/value.
  *
  * @param path
  *   The full path to the AVRO file.
  * @param key
  *   The record field/header to find.
  * @param value
  *   The value of the record field/header to find.
  * @return
  *   The object.
  * @throws IOException
  *   Thrown while reading/deserializing the AVRO file.
  ****************************************************************************/
  private static GenericRecord searchAvroFile(final String path, String key, final String value) throws IOException
  {
    final File avroFile = new File(path);
    final DatumReader<GenericRecord> datumReader = new ReflectDatumReader<GenericRecord>();
    final DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
    final String[] nestedKeys = key.split("\\.");

    key = nestedKeys[nestedKeys.length - 1];
    GenericRecord record = null;
    GenericRecord nestedRecord = null;

    while (fileReader.hasNext())
    {
      // Reuse user object by passing it to next(). This reduces object
      // allocation and garbage collection for large files.
      record = fileReader.next(record);

      if (nestedKeys.length > 1)
        nestedRecord = traverseRecords(record, nestedKeys);
      else
        nestedRecord = record;

      if (isMatch(nestedRecord, key, value))
        return record;
    }

    return null;
  }

  /****************************************************************************
  * Iterates nested AVRO records searching for the full path of keys.
  *
  * @param record
  *   The AVRO record.
  * @param keys
  *   The record keys/values.
  * @return
  *   The nested record containing the final key.
  ****************************************************************************/
  private static GenericRecord traverseRecords(GenericRecord record, final String[] keys)
  {
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++)
    {
      if (keyIndex == keys.length - 1)
        return record;
      else
      {
        record = (GenericRecord)record.get(keys[keyIndex]);

        // If it's a nullable field that's missing, skip the rest of the
        // record
        if (record == null)
          return null;
      }
    }

    return record;
  }

  /****************************************************************************
  * Writes out the JSON representation of the complete AVRO record containing
  * the key/value pair that was sought.
  *
  * @param found
  *   The AVRO record that contains the key/value pair.
  * @throws IOException
  *   Thrown while reading/deserializing the AVRO file or writing the JSON.
  ****************************************************************************/
  private static void writeJson(final GenericRecord found) throws IOException
  {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final DatumWriter<GenericRecord> writer = new GenericDatumWriter();
    final Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(found.getSchema(), outputStream);
    writer.setSchema(found.getSchema());
    writer.write(found, jsonEncoder);
    jsonEncoder.flush();
    System.out.print(outputStream.toString());
  }
}