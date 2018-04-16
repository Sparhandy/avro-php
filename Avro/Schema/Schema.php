<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Util;

class Schema
{
    public const NULL_TYPE = 'null';
    public const BOOLEAN_TYPE = 'boolean';
    public const INT_TYPE = 'int';
    public const LONG_TYPE = 'long';
    public const FLOAT_TYPE = 'float';
    public const DOUBLE_TYPE = 'double';
    public const BYTES_TYPE = 'bytes';
    public const STRING_TYPE = 'string';

    public const RECORD_SCHEMA = 'record';
    public const ENUM_SCHEMA = 'enum';
    public const ARRAY_SCHEMA = 'array';
    public const MAP_SCHEMA = 'map';
    public const UNION_SCHEMA = 'union';
    public const FIXED_SCHEMA = 'fixed';

    public const ERROR_SCHEMA = 'error';
    public const ERROR_UNION_SCHEMA = 'error_union';
    public const REQUEST_SCHEMA = 'request';

    public const TYPE_ATTR = 'type';
    public const NAME_ATTR = 'name';
    public const NAMESPACE_ATTR = 'namespace';
    public const FULLNAME_ATTR = 'fullname';
    public const SIZE_ATTR = 'size';
    public const FIELDS_ATTR = 'fields';
    public const ITEMS_ATTR = 'items';
    public const SYMBOLS_ATTR = 'symbols';
    public const VALUES_ATTR = 'values';
    public const DOC_ATTR = 'doc';

    private const INT_MIN_VALUE = -2147483648;
    private const INT_MAX_VALUE = 2147483647;
    private const LONG_MIN_VALUE = -4611686018427387904;
    private const LONG_MAX_VALUE = 4611686018427387903;

    private static $primitiveTypes = [
        self::NULL_TYPE,
        self::BOOLEAN_TYPE,
        self::INT_TYPE,
        self::LONG_TYPE,
        self::FLOAT_TYPE,
        self::DOUBLE_TYPE,
        self::BYTES_TYPE,
        self::STRING_TYPE,
    ];

    private static $complexTypes = [
        self::RECORD_SCHEMA,
        self::ENUM_SCHEMA,
        self::ARRAY_SCHEMA,
        self::MAP_SCHEMA,
        self::UNION_SCHEMA,
        self::FIXED_SCHEMA,
    ];

    private static $namedTypes = [
        self::RECORD_SCHEMA,
        self::ENUM_SCHEMA,
        self::FIXED_SCHEMA,
    ];

    private static $unnamedTypes = [
        self::ARRAY_SCHEMA,
        self::MAP_SCHEMA,
        self::UNION_SCHEMA,
    ];

    /**
     * @internal Should only be called from within the constructor of a class which extends Schema
     *
     * @param string $type a schema type name
     */
    public function __construct($type)
    {
        $this->type = $type;
    }

    public function __toString()
    {
        return json_encode($this->toAvro());
    }

    public static function isPrimitiveType(?string $type): bool
    {
        return in_array($type, self::$primitiveTypes, true);
    }

    public static function isComplexType(?string $type): bool
    {
        return in_array($type, self::$complexTypes, true);
    }

    public static function isNamedType(?string $type): bool
    {
        return in_array($type, self::$namedTypes, true);
    }

    public static function isUnnamedType(?string $type): bool
    {
        return in_array($type, self::$unnamedTypes, true);
    }

    public static function isValidType(?string $type): bool
    {
        return self::isPrimitiveType($type)
            || self::isComplexType($type)
            || in_array($type, [
                self::ERROR_SCHEMA,
                self::ERROR_UNION_SCHEMA,
                self::REQUEST_SCHEMA,
            ], true);
    }

    public static function parse(string $json): self
    {
        $schemata = new NamedSchemata();

        return self::realParse(json_decode($json, true), null, $schemata);
    }

    public static function realParse($avro, ?string $defaultNamespace = null, NamedSchemata &$schemata = null): ?self
    {
        $schemata = $schemata ?? new NamedSchemata();

        if (is_array($avro)) {
            $type = self::extractType($avro);

            if (self::isPrimitiveType($type)) {
                return new PrimitiveSchema($type);
            }

            if (self::isComplexType($type)) {
                switch ($type) {
                    case self::RECORD_SCHEMA:
                        return new RecordSchema(
                            new Name(
                                Util::arrayValue($avro, self::NAME_ATTR),
                                Util::arrayValue($avro, self::NAMESPACE_ATTR),
                                $defaultNamespace
                            ),
                            Util::arrayValue($avro, self::DOC_ATTR),
                            Util::arrayValue($avro, self::FIELDS_ATTR),
                            $schemata,
                            $type
                        );
                    case self::ENUM_SCHEMA:
                        return new EnumSchema(
                            new Name(
                                Util::arrayValue($avro, self::NAME_ATTR),
                                Util::arrayValue($avro, self::NAMESPACE_ATTR),
                                $defaultNamespace
                            ),
                            Util::arrayValue($avro, self::DOC_ATTR),
                            Util::arrayValue($avro, self::SYMBOLS_ATTR),
                            $schemata
                        );
                    case self::ARRAY_SCHEMA:
                        return new ArraySchema($avro[self::ITEMS_ATTR], $defaultNamespace, $schemata);
                    case self::MAP_SCHEMA:
                        return new MapSchema($avro[self::VALUES_ATTR], $defaultNamespace, $schemata);
                    case self::UNION_SCHEMA:
                        return new UnionSchema($avro, $defaultNamespace, $schemata);
                    case self::FIXED_SCHEMA:
                        return new FixedSchema(
                            new Name(
                                Util::arrayValue($avro, self::NAME_ATTR),
                                Util::arrayValue($avro, self::NAMESPACE_ATTR),
                                $defaultNamespace
                            ),
                            Util::arrayValue($avro, self::DOC_ATTR),
                            Util::arrayValue($avro, self::SIZE_ATTR),
                            $schemata
                        );
                    default:
                        throw new SchemaParseException(sprintf('Unknown complex type: %s', $type));
                }
            }

            if (self::ERROR_SCHEMA === $type) {
                return new RecordSchema(
                    new Name(
                        Util::arrayValue($avro, self::NAME_ATTR),
                        Util::arrayValue($avro, self::NAMESPACE_ATTR),
                        $defaultNamespace
                    ),
                    Util::arrayValue($avro, self::DOC_ATTR),
                    Util::arrayValue($avro, self::FIELDS_ATTR),
                    $schemata,
                    $type
                );
            }

            if (self::isValidType($type)) {
                throw new SchemaParseException(sprintf('Unknown valid type: %s', $type));
            }

            throw new SchemaParseException(sprintf('Undefined type: %s', $type));
        }

        if (self::isPrimitiveType($avro)) {
            return new PrimitiveSchema($avro);
        }

        throw new SchemaParseException(sprintf('%s is not a schema we know about.', print_r($avro, true)));
    }

    public static function isValidDatum(self $expectedSchema, $datum): ?bool
    {
        switch ($expectedSchema->type) {
            case self::NULL_TYPE:
                return null === $datum;
            case self::BOOLEAN_TYPE:
                return is_bool($datum);
            case self::STRING_TYPE:
            case self::BYTES_TYPE:
                return is_string($datum);
            case self::INT_TYPE:
                return is_int($datum) && (self::INT_MIN_VALUE <= $datum) && ($datum <= self::INT_MAX_VALUE);
            case self::LONG_TYPE:
                return is_int($datum) && (self::LONG_MIN_VALUE <= $datum) && ($datum <= self::LONG_MAX_VALUE);
            case self::FLOAT_TYPE:
            case self::DOUBLE_TYPE:
                return is_float($datum) || is_int($datum);
            case self::ARRAY_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $d) {
                        if (!self::isValidDatum($expectedSchema->items(), $d)) {
                            return false;
                        }
                    }

                    return true;
                }

                return false;
            case self::MAP_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $k => $v) {
                        if (!is_string($k) || !self::isValidDatum($expectedSchema->values(), $v)) {
                            return false;
                        }
                    }

                    return true;
                }

                return false;
            case self::UNION_SCHEMA:
                foreach ($expectedSchema->schemas() as $schema) {
                    if (self::isValidDatum($schema, $datum)) {
                        return true;
                    }
                }

                return false;
            case self::ENUM_SCHEMA:
                return in_array($datum, $expectedSchema->symbols(), true);
            case self::FIXED_SCHEMA:
                return is_string($datum) && strlen($datum) === $expectedSchema->size();
            case self::RECORD_SCHEMA:
            case self::ERROR_SCHEMA:
            case self::REQUEST_SCHEMA:
                if (is_array($datum)) {
                    foreach ($expectedSchema->fields() as $field) {
                        if ($field->hasDefaultValue() && !isset($datum[$field->name()])) {
                            $value = $field->defaultValue();
                        } else {
                            $value = $datum[$field->name()];
                        }
                    }

                    return !((!$field->hasDefaultValue() && !array_key_exists($field->name(), $datum))
                        || !self::isValidDatum($field->type(), $value));
                }

                return false;
            default:
                throw new SchemaParseException(sprintf('%s is not allowed.', $expectedSchema));
        }
    }

    public function type()
    {
        return $this->type;
    }

    public function toAvro()
    {
        return [self::TYPE_ATTR => $this->type];
    }

    public function attribute(string $attribute): string
    {
        return $this->$attribute();
    }

    protected static function subparse($avro, ?string $defaultNamespace, NamedSchemata &$schemata = null): ?self
    {
        try {
            return self::realParse($avro, $defaultNamespace, $schemata);
        } catch (SchemaParseException $e) {
            throw $e;
        } catch (\Exception $e) {
            throw new SchemaParseException(
                sprintf('Sub-schema is not a valid Avro schema. Bad schema: %s', print_r($avro, true))
            );
        }
    }

    private static function extractType($avro): ?string
    {
        $type = Util::arrayValue($avro, self::TYPE_ATTR);

        if (null === $type && Util::isList($avro)) {
            $type = self::UNION_SCHEMA;
        }

        return $type;
    }
}
