import { NativeAttributeValue } from '@aws-sdk/lib-dynamodb';
import { ItemParseError } from './errors';

type Item = Record<string, NativeAttributeValue>;

function unexpectedType(key: string, value: NativeAttributeValue): ItemParseError {
  return new ItemParseError(key, `Unexpected ${typeof value} data type for ${key}`);
}

function isOneOf<T extends string>(value: string, validValues: ReadonlyArray<T>): value is T {
  return validValues.some((valid) => valid === value);
}

/** A map attribute: not null, not a list. Sets are objects too and are accepted. */
function isObject(value: NativeAttributeValue): boolean {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Readers for item attributes. Each throws `ItemParseError` for a missing required attribute
 * or a value of the wrong type.
 *
 * - `extractX` requires the attribute.
 * - `extractOptionalX` returns `undefined` when it is absent; a stored NULL is an error.
 * - `extractNullableX` returns `undefined` when it is absent and `null` when it is stored as
 *   NULL, for tables that write NULL to clear an attribute.
 */
export class ItemParser {
  static extractString(key: string, item: Item): string {
    const value = item[key];
    if (typeof value !== 'string') {
      throw unexpectedType(key, value);
    }
    return value;
  }

  static extractStringLiteral<T extends string>(key: string, item: Item, validValues: ReadonlyArray<T>): T {
    const value = item[key];
    if (typeof value !== 'string' || !isOneOf(value, validValues)) {
      throw unexpectedType(key, value);
    }
    return value;
  }

  static extractOptionalString(key: string, item: Item): string | undefined {
    return item[key] === undefined ? undefined : ItemParser.extractString(key, item);
  }

  static extractOptionalStringLiteral<T extends string>(key: string, item: Item, validValues: ReadonlyArray<T>): T | undefined {
    const value = item[key];
    if (value === undefined) {
      return undefined;
    }
    if (typeof value !== 'string') {
      throw unexpectedType(key, value);
    }
    if (!isOneOf(value, validValues)) {
      throw new ItemParseError(key, `Unexpected ${value} for ${key}`);
    }
    return value;
  }

  static extractNumber(key: string, item: Item): number {
    const value = item[key];
    if (typeof value !== 'number') {
      throw unexpectedType(key, value);
    }
    return value;
  }

  static extractBoolean(key: string, item: Item): boolean {
    const value = item[key];
    if (typeof value !== 'boolean') {
      throw unexpectedType(key, value);
    }
    return value;
  }

  static extractOptionalNumber(key: string, item: Item): number | undefined {
    return item[key] === undefined ? undefined : ItemParser.extractNumber(key, item);
  }

  static extractOptionalBoolean(key: string, item: Item): boolean | undefined {
    return item[key] === undefined ? undefined : ItemParser.extractBoolean(key, item);
  }

  static extractISODateString(key: string, item: Item): string {
    const value = ItemParser.extractString(key, item);
    if (isNaN(new Date(value).getTime())) {
      throw new ItemParseError(key, `${value} is not a valid ISO Date String.`);
    }
    return value;
  }

  static extractOptionalISODateString(key: string, item: Item): string | undefined {
    return item[key] === undefined ? undefined : ItemParser.extractISODateString(key, item);
  }

  static extractArray<T>(key: string, item: Item, build: (x: NativeAttributeValue) => T): T[] {
    const value = item[key];
    if (!Array.isArray(value)) {
      throw unexpectedType(key, value);
    }
    return value.map((entry) => build(entry));
  }

  static extractOptionalArray<T>(key: string, item: Item, build: (x: NativeAttributeValue) => T): T[] | undefined {
    return item[key] === undefined ? undefined : ItemParser.extractArray(key, item, build);
  }

  static extractObject<T>(key: string, item: Item, build: (x: NativeAttributeValue) => T): T {
    const value = item[key];
    if (!isObject(value)) {
      throw new ItemParseError(key, `Unexpected ${value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value} data type for ${key}`);
    }
    return build(value);
  }

  static extractOptionalObject<T>(key: string, item: Item, build: (x: NativeAttributeValue) => T): T | undefined {
    return item[key] === undefined ? undefined : ItemParser.extractObject(key, item, build);
  }

  static extractNullableString(key: string, item: Item): string | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalString(key, item);
  }

  static extractNullableStringLiteral<T extends string>(key: string, item: Item, validValues: ReadonlyArray<T>): T | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalStringLiteral(key, item, validValues);
  }

  static extractNullableNumber(key: string, item: Item): number | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalNumber(key, item);
  }

  static extractNullableBoolean(key: string, item: Item): boolean | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalBoolean(key, item);
  }

  static extractNullableISODateString(key: string, item: Item): string | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalISODateString(key, item);
  }

  static extractNullableArray<T>(key: string, item: Item, build: (x: NativeAttributeValue) => T): T[] | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalArray(key, item, build);
  }

  static extractNullableObject<T>(key: string, item: Item, build: (x: NativeAttributeValue) => T): T | null | undefined {
    return item[key] === null ? null : ItemParser.extractOptionalObject(key, item, build);
  }
}
