<?php

namespace Avro\DataIO;

use PHPUnit\Framework\TestCase;

class DataFileTest extends TestCase
{
    private const REMOVE_DATA_FILES = true;

    private $tmpDir;
    private $dataFiles = [];

    protected function setUp(): void
    {
        $this->tmpDir = sys_get_temp_dir();
        if (!file_exists($this->tmpDir)) {
            mkdir($this->tmpDir);
        }
        $this->removeDataFiles();
    }

    protected function tearDown(): void
    {
        $this->removeDataFiles();
    }

    public static function currentTimestamp(): string
    {
        return strftime('%Y%m%dT%H%M%S');
    }

    public function testWriteReadNothingRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-nothing-null-%s.avr', $codec));
            $writersSchema = '"null"';
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $drData = (array) $dr->data();
            $readData = array_shift($drData);
            $dr->close();
            $this->assertNull($readData);
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testWriteReadNullRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-null-%s.avr', $codec));
            $writersSchema = '"null"';
            $data = null;
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            $dw->append($data);
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $drData = (array)$dr->data();
            $readData = array_shift($drData);
            $dr->close();
            $this->assertSame($data, $readData);
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testWriteReadStringRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-str-%s.avr', $codec));
            $writersSchema = '"string"';
            $data = 'foo';
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            $dw->append($data);
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $drData = (array)$dr->data();
            $readData = array_shift($drData);
            $dr->close();
            $this->assertSame($data, $readData);
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testWriteReadRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-int-%s.avr', $codec));
            $writersSchema = '"int"';
            $data = 1;

            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            $dw->append(1);
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $drData = (array)$dr->data();
            $readData = array_shift($drData);
            $dr->close();
            $this->assertSame($data, $readData);
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testWriteReadTrueRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-true-%s.avr', $codec));
            $writersSchema = '"boolean"';
            $datum = true;
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            $dw->append($datum);
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $drData = (array)$dr->data();
            $readDatum = array_shift($drData);
            $dr->close();
            $this->assertSame($datum, $readDatum);
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testWriteReadFalseRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-false-%s.avr', $codec));
            $writersSchema = '"boolean"';
            $datum = false;
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            $dw->append($datum);
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $drData = (array)$dr->data();
            $readDatum = array_shift($drData);
            $dr->close();
            $this->assertSame($datum, $readDatum);
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testWriteReadIntArrayRoundTrip(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-wr-int-ary-%s.avr', $codec));
            $writersSchema = '"int"';
            $data = [10, 20, 30, 40, 50, 60, 70];
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            foreach ($data as $datum) {
                $dw->append($datum);
            }
            $dw->close();

            $dr = DataIO::openFile($dataFile);
            $readData = $dr->data();
            $dr->close();
            $this->assertSame(
                $data,
                $readData,
                sprintf(
                    "in: %s\nout: %s",
                    json_encode($data),
                    json_encode($readData)
                )
            );
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals($writersSchema, $dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR));
        }
    }

    public function testDifferingSchemasWithPrimitives(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-prim-%s.avr', $codec));

            $writersSchema = <<<JSON
{ "type": "record",
  "name": "User",
  "fields" : [
      {"name": "username", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "verified", "type": "boolean", "default": "false"}
      ]}
JSON;
            $data = [
                ['username' => 'john', 'age' => 25, 'verified' => true],
                ['username' => 'ryan', 'age' => 23, 'verified' => false],
            ];
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            foreach ($data as $datum) {
                $dw->append($datum);
            }
            $dw->close();
            $readerSchema = <<<JSON
      { "type": "record",
        "name": "User",
        "fields" : [
      {"name": "username", "type": "string"}
      ]}
JSON;
            $dr = DataIO::openFile($dataFile, 'r', $readerSchema);
            foreach ($dr->data() as $index => $record) {
                $this->assertSame($data[$index]['username'], $record['username']);
            }
            $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
            $this->assertEquals(json_decode($writersSchema, true), json_decode($dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR), true));
        }
    }

    public function testDifferingSchemasWithComplexObjects(): void
    {
        foreach (DataIO::getValidCodecs() as $codec) {
            $dataFile = $this->addDataFile(sprintf('data-complex-%s.avr', $codec));

            $writersSchema = <<<JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_fixed", "type": {"name": "inner_fixed",
                                         "type": "fixed", "size": 3}},
    {"name": "something_enum", "type": {"name": "inner_enum",
                                        "type": "enum",
                                        "symbols": ["hello", "goodbye"]}},
    {"name": "something_array", "type": {"type": "array", "items": "int"}},
    {"name": "something_map", "type": {"type": "map", "values": "int"}},
    {"name": "something_record", "type": {"name": "inner_record",
                                          "type": "record",
                                          "fields": [
                                            {"name": "inner", "type": "int"}
                                          ]}},
    {"name": "username", "type": "string"}
]}
JSON;

            $data = [
                [
                    'username' => 'john',
                    'something_fixed' => 'foo',
                    'something_enum' => 'hello',
                    'something_array' => [1, 2, 3],
                    'something_map' => ['a' => 1, 'b' => 2],
                    'something_record' => ['inner' => 2],
                    'something_error' => ['code' => 403],
                ],
                [
                    'username' => 'ryan',
                    'something_fixed' => 'bar',
                    'something_enum' => 'goodbye',
                    'something_array' => [1, 2, 3],
                    'something_map' => ['a' => 2, 'b' => 6],
                    'something_record' => ['inner' => 1],
                    'something_error' => ['code' => 401],
                ],
            ];
            $dw = DataIO::openFile($dataFile, 'w', $writersSchema, $codec);
            foreach ($data as $datum) {
                $dw->append($datum);
            }
            $dw->close();

            foreach ([
                         'fixed',
                         'enum',
                         'record',
                         'error',
                         'array',
                         'map',
                         'union',
                     ] as $s) {
                $readersSchema = json_decode($writersSchema, true);
                $dr = DataIO::openFile($dataFile, 'r', json_encode($readersSchema));
                foreach ($dr->data() as $idx => $obj) {
                    foreach ($readersSchema['fields'] as $field) {
                        $fieldName = $field['name'];
                        $this->assertSame($data[$idx][$fieldName], $obj[$fieldName]);
                    }
                }
                $dr->close();
                $this->assertEquals($codec, $dr->getMetaDataFor(DataIO::METADATA_CODEC_ATTR));
                $this->assertEquals(json_decode($writersSchema, true), json_decode($dr->getMetaDataFor(DataIO::METADATA_SCHEMA_ATTR), true));
            }
        }
    }

    protected function addDataFile($dataFile): string
    {
        $dataFile = "$dataFile.".self::currentTimestamp();
        $full = $this->tmpDir.DIRECTORY_SEPARATOR.$dataFile;
        $this->dataFiles[] = $full;

        return $full;
    }

    protected static function removeDataFile($dataFile): void
    {
        if (file_exists($dataFile)) {
            unlink($dataFile);
        }
    }

    protected function removeDataFiles(): void
    {
        if (self::REMOVE_DATA_FILES && !empty($this->dataFiles)) {
            foreach ($this->dataFiles as $dataFile) {
                static::removeDataFile($dataFile);
            }
        }
    }
}
