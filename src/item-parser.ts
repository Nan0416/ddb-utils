import { NativeAttributeValue } from '@aws-sdk/lib-dynamodb';

export class ItemParser {
  static extractString(key: string, item: Record<string, NativeAttributeValue>): string {
    const value = item[key];
    if (typeof value !== 'string') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }
    return value;
  }

  static extractStringLiteral<T extends string>(key: string, item: Record<string, NativeAttributeValue>, validValues: ReadonlyArray<T>): T {
    const value = item[key];
    if (typeof value !== 'string' || !validValues.includes(value as any)) {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }

    return value as any;
  }

  static extractOptionalString(key: string, item: Record<string, NativeAttributeValue>): string | undefined {
    const value = item[key];
    if (value === undefined) {
      return undefined;
    }

    if (typeof value !== 'string') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }
    return value;
  }

  static extractOptionalStringLiteral<T extends string>(key: string, item: Record<string, NativeAttributeValue>, assertion: (x: any) => asserts x is T): T | undefined {
    const value = item[key];
    if (value === undefined) {
      return undefined;
    }
    if (typeof value !== 'string') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }
    assertion(value);
    return value;
  }

  static extractNumber(key: string, item: Record<string, NativeAttributeValue>): number {
    const value = item[key];
    if (typeof value !== 'number') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }
    return value;
  }

  static extractBoolean(key: string, item: Record<string, NativeAttributeValue>): boolean {
    const value = item[key];
    if (typeof value !== 'boolean') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }
    return value;
  }

  static extractOptionalNumber(key: string, item: Record<string, NativeAttributeValue>): number | undefined {
    const value = item[key];
    if (value === undefined) {
      return undefined;
    }

    if (typeof value !== 'number') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }
    return value;
  }

  static extractISODateString(key: string, item: Record<string, NativeAttributeValue>): string {
    const value = item[key];
    if (typeof value !== 'string') {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }

    if (isNaN(new Date(value).getTime())) {
      throw new Error(`${value} is not a valid ISO Date String.`);
    }
    return value;
  }

  static extractArray<T>(key: string, item: Record<string, NativeAttributeValue>, build: (x: NativeAttributeValue) => T): T[] {
    const value = item[key];
    if (!Array.isArray(value)) {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }

    const results: T[] = [];
    for (let i = 0; i < value.length; i++) {
      results.push(build(value[i]));
    }
    return results;
  }

  static extractOptionalArray<T>(key: string, item: Record<string, NativeAttributeValue>, build: (x: NativeAttributeValue) => T): T[] | undefined {
    const value = item[key];
    if (value === undefined) {
      return undefined;
    }
    if (!Array.isArray(value)) {
      throw new Error(`Unexpected ${typeof value} data type for ${key}`);
    }

    const results: T[] = [];
    for (let i = 0; i < value.length; i++) {
      results.push(build(value[i]));
    }
    return results;
  }

  static extractObject<T>(key: string, item: Record<string, NativeAttributeValue>, build: (x: NativeAttributeValue) => T): T {
    const object = item[key];
    if (typeof object === 'object') {
      return build(object);
    } else {
      throw new Error(`Unexpected ${typeof object} data type for ${key}`);
    }
  }

  static extractOptionalObject<T>(key: string, item: Record<string, NativeAttributeValue>, build: (x: NativeAttributeValue) => T): T | undefined {
    const object = item[key];
    if (object === undefined) {
      return undefined;
    }

    if (typeof object === 'object') {
      return build(object);
    } else {
      throw new Error(`Unexpected ${typeof object} data type for ${key}`);
    }
  }
}
