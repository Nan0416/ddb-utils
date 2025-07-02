import { ItemParser } from '../src/item-parser';
import { NativeAttributeValue } from '@aws-sdk/lib-dynamodb';

describe('item-parser', () => {
  describe('extractString', () => {
    test('should extract string value successfully', () => {
      const item = { name: 'John Doe', age: 30 };
      const result = ItemParser.extractString('name', item);
      expect(result).toBe('John Doe');
    });

    test('should throw error for non-string value', () => {
      const item = { name: 123, age: 30 };
      expect(() => {
        ItemParser.extractString('name', item);
      }).toThrow('Unexpected number data type for name');
    });

    test('should throw error for undefined value', () => {
      const item = { age: 30 };
      expect(() => {
        ItemParser.extractString('name', item);
      }).toThrow('Unexpected undefined data type for name');
    });

    test('should throw error for null value', () => {
      const item = { name: null, age: 30 };
      expect(() => {
        ItemParser.extractString('name', item);
      }).toThrow('Unexpected object data type for name');
    });
  });

  describe('extractStringLiteral', () => {
    const validValues = ['active', 'inactive', 'pending'] as const;

    test('should extract valid string literal', () => {
      const item = { status: 'active', name: 'John' };
      const result = ItemParser.extractStringLiteral('status', item, validValues);
      expect(result).toBe('active');
    });

    test('should throw error for invalid string literal', () => {
      const item = { status: 'invalid', name: 'John' };
      expect(() => {
        ItemParser.extractStringLiteral('status', item, validValues);
      }).toThrow('Unexpected string data type for status');
    });

    test('should throw error for non-string value', () => {
      const item = { status: 123, name: 'John' };
      expect(() => {
        ItemParser.extractStringLiteral('status', item, validValues);
      }).toThrow('Unexpected number data type for status');
    });

    test('should throw error for undefined value', () => {
      const item = { name: 'John' };
      expect(() => {
        ItemParser.extractStringLiteral('status', item, validValues);
      }).toThrow('Unexpected undefined data type for status');
    });
  });

  describe('extractOptionalString', () => {
    test('should extract string value successfully', () => {
      const item = { name: 'John Doe', age: 30 };
      const result = ItemParser.extractOptionalString('name', item);
      expect(result).toBe('John Doe');
    });

    test('should return undefined for missing key', () => {
      const item = { age: 30 };
      const result = ItemParser.extractOptionalString('name', item);
      expect(result).toBeUndefined();
    });

    test('should return undefined for undefined value', () => {
      const item = { name: undefined, age: 30 };
      const result = ItemParser.extractOptionalString('name', item);
      expect(result).toBeUndefined();
    });

    test('should throw error for non-string value', () => {
      const item = { name: 123, age: 30 };
      expect(() => {
        ItemParser.extractOptionalString('name', item);
      }).toThrow('Unexpected number data type for name');
    });

    test('should throw error for null value', () => {
      const item = { name: null, age: 30 };
      expect(() => {
        ItemParser.extractOptionalString('name', item);
      }).toThrow('Unexpected object data type for name');
    });
  });

  describe('extractOptionalStringLiteral', () => {
    const assertion = (x: any): asserts x is 'active' | 'inactive' => {
      if (x !== 'active' && x !== 'inactive') {
        throw new Error('Invalid status');
      }
    };

    test('should extract valid string literal', () => {
      const item = { status: 'active', name: 'John' };
      const result = ItemParser.extractOptionalStringLiteral('status', item, assertion);
      expect(result).toBe('active');
    });

    test('should return undefined for missing key', () => {
      const item = { name: 'John' };
      const result = ItemParser.extractOptionalStringLiteral('status', item, assertion);
      expect(result).toBeUndefined();
    });

    test('should return undefined for undefined value', () => {
      const item = { status: undefined, name: 'John' };
      const result = ItemParser.extractOptionalStringLiteral('status', item, assertion);
      expect(result).toBeUndefined();
    });

    test('should throw error for invalid string literal', () => {
      const item = { status: 'invalid', name: 'John' };
      expect(() => {
        ItemParser.extractOptionalStringLiteral('status', item, assertion);
      }).toThrow('Invalid status');
    });

    test('should throw error for non-string value', () => {
      const item = { status: 123, name: 'John' };
      expect(() => {
        ItemParser.extractOptionalStringLiteral('status', item, assertion);
      }).toThrow('Unexpected number data type for status');
    });
  });

  describe('extractNumber', () => {
    test('should extract number value successfully', () => {
      const item = { age: 30, name: 'John' };
      const result = ItemParser.extractNumber('age', item);
      expect(result).toBe(30);
    });

    test('should extract zero value', () => {
      const item = { count: 0, name: 'John' };
      const result = ItemParser.extractNumber('count', item);
      expect(result).toBe(0);
    });

    test('should extract negative number', () => {
      const item = { temperature: -5, name: 'John' };
      const result = ItemParser.extractNumber('temperature', item);
      expect(result).toBe(-5);
    });

    test('should throw error for non-number value', () => {
      const item = { age: '30', name: 'John' };
      expect(() => {
        ItemParser.extractNumber('age', item);
      }).toThrow('Unexpected string data type for age');
    });

    test('should throw error for undefined value', () => {
      const item = { name: 'John' };
      expect(() => {
        ItemParser.extractNumber('age', item);
      }).toThrow('Unexpected undefined data type for age');
    });

    test('should throw error for null value', () => {
      const item = { age: null, name: 'John' };
      expect(() => {
        ItemParser.extractNumber('age', item);
      }).toThrow('Unexpected object data type for age');
    });
  });

  describe('extractBoolean', () => {
    test('should extract true value', () => {
      const item = { active: true, name: 'John' };
      const result = ItemParser.extractBoolean('active', item);
      expect(result).toBe(true);
    });

    test('should extract false value', () => {
      const item = { active: false, name: 'John' };
      const result = ItemParser.extractBoolean('active', item);
      expect(result).toBe(false);
    });

    test('should throw error for non-boolean value', () => {
      const item = { active: 'true', name: 'John' };
      expect(() => {
        ItemParser.extractBoolean('active', item);
      }).toThrow('Unexpected string data type for active');
    });

    test('should throw error for undefined value', () => {
      const item = { name: 'John' };
      expect(() => {
        ItemParser.extractBoolean('active', item);
      }).toThrow('Unexpected undefined data type for active');
    });

    test('should throw error for null value', () => {
      const item = { active: null, name: 'John' };
      expect(() => {
        ItemParser.extractBoolean('active', item);
      }).toThrow('Unexpected object data type for active');
    });
  });

  describe('extractOptionalNumber', () => {
    test('should extract number value successfully', () => {
      const item = { age: 30, name: 'John' };
      const result = ItemParser.extractOptionalNumber('age', item);
      expect(result).toBe(30);
    });

    test('should return undefined for missing key', () => {
      const item = { name: 'John' };
      const result = ItemParser.extractOptionalNumber('age', item);
      expect(result).toBeUndefined();
    });

    test('should return undefined for undefined value', () => {
      const item = { age: undefined, name: 'John' };
      const result = ItemParser.extractOptionalNumber('age', item);
      expect(result).toBeUndefined();
    });

    test('should throw error for non-number value', () => {
      const item = { age: '30', name: 'John' };
      expect(() => {
        ItemParser.extractOptionalNumber('age', item);
      }).toThrow('Unexpected string data type for age');
    });

    test('should throw error for null value', () => {
      const item = { age: null, name: 'John' };
      expect(() => {
        ItemParser.extractOptionalNumber('age', item);
      }).toThrow('Unexpected object data type for age');
    });
  });

  describe('extractISODateString', () => {
    test('should extract valid ISO date string', () => {
      const item = { createdAt: '2024-01-01T00:00:00.000Z', name: 'John' };
      const result = ItemParser.extractISODateString('createdAt', item);
      expect(result).toBe('2024-01-01T00:00:00.000Z');
    });

    test('should extract valid date string without time', () => {
      const item = { createdAt: '2024-01-01', name: 'John' };
      const result = ItemParser.extractISODateString('createdAt', item);
      expect(result).toBe('2024-01-01');
    });

    test('should throw error for invalid date string', () => {
      const item = { createdAt: 'invalid-date', name: 'John' };
      expect(() => {
        ItemParser.extractISODateString('createdAt', item);
      }).toThrow('invalid-date is not a valid ISO Date String.');
    });

    test('should throw error for non-string value', () => {
      const item = { createdAt: 123, name: 'John' };
      expect(() => {
        ItemParser.extractISODateString('createdAt', item);
      }).toThrow('Unexpected number data type for createdAt');
    });

    test('should throw error for undefined value', () => {
      const item = { name: 'John' };
      expect(() => {
        ItemParser.extractISODateString('createdAt', item);
      }).toThrow('Unexpected undefined data type for createdAt');
    });
  });

  describe('extractArray', () => {
    const buildString = (x: NativeAttributeValue): string => {
      if (typeof x !== 'string') {
        throw new Error('Expected string');
      }
      return x;
    };

    test('should extract array of strings', () => {
      const item = { tags: ['tag1', 'tag2', 'tag3'], name: 'John' };
      const result = ItemParser.extractArray('tags', item, buildString);
      expect(result).toEqual(['tag1', 'tag2', 'tag3']);
    });

    test('should extract empty array', () => {
      const item = { tags: [], name: 'John' };
      const result = ItemParser.extractArray('tags', item, buildString);
      expect(result).toEqual([]);
    });

    test('should throw error for non-array value', () => {
      const item = { tags: 'not-an-array', name: 'John' };
      expect(() => {
        ItemParser.extractArray('tags', item, buildString);
      }).toThrow('Unexpected string data type for tags');
    });

    test('should throw error for undefined value', () => {
      const item = { name: 'John' };
      expect(() => {
        ItemParser.extractArray('tags', item, buildString);
      }).toThrow('Unexpected undefined data type for tags');
    });

    test('should throw error when build function fails', () => {
      const item = { tags: ['tag1', 123, 'tag3'], name: 'John' };
      expect(() => {
        ItemParser.extractArray('tags', item, buildString);
      }).toThrow('Expected string');
    });
  });

  describe('extractOptionalArray', () => {
    const buildString = (x: NativeAttributeValue): string => {
      if (typeof x !== 'string') {
        throw new Error('Expected string');
      }
      return x;
    };

    test('should extract array of strings', () => {
      const item = { tags: ['tag1', 'tag2', 'tag3'], name: 'John' };
      const result = ItemParser.extractOptionalArray('tags', item, buildString);
      expect(result).toEqual(['tag1', 'tag2', 'tag3']);
    });

    test('should return undefined for missing key', () => {
      const item = { name: 'John' };
      const result = ItemParser.extractOptionalArray('tags', item, buildString);
      expect(result).toBeUndefined();
    });

    test('should return undefined for undefined value', () => {
      const item = { tags: undefined, name: 'John' };
      const result = ItemParser.extractOptionalArray('tags', item, buildString);
      expect(result).toBeUndefined();
    });

    test('should extract empty array', () => {
      const item = { tags: [], name: 'John' };
      const result = ItemParser.extractOptionalArray('tags', item, buildString);
      expect(result).toEqual([]);
    });

    test('should throw error for non-array value', () => {
      const item = { tags: 'not-an-array', name: 'John' };
      expect(() => {
        ItemParser.extractOptionalArray('tags', item, buildString);
      }).toThrow('Unexpected string data type for tags');
    });

    test('should throw error when build function fails', () => {
      const item = { tags: ['tag1', 123, 'tag3'], name: 'John' };
      expect(() => {
        ItemParser.extractOptionalArray('tags', item, buildString);
      }).toThrow('Expected string');
    });
  });

  describe('extractObject', () => {
    interface User {
      id: string;
      name: string;
    }

    const buildUser = (x: NativeAttributeValue): User => {
      if (typeof x === 'object' && x !== null && !Array.isArray(x)) {
        const obj = x as Record<string, NativeAttributeValue>;
        return {
          id: ItemParser.extractString('id', obj),
          name: ItemParser.extractString('name', obj),
        };
      }
      throw new Error('Expected object');
    };

    test('should extract object successfully', () => {
      const item = {
        user: { id: '123', name: 'John Doe' },
        status: 'active',
      };
      const result = ItemParser.extractObject('user', item, buildUser);
      expect(result).toEqual({ id: '123', name: 'John Doe' });
    });

    test('should throw error for non-object value', () => {
      const item = { user: 'not-an-object', status: 'active' };
      expect(() => {
        ItemParser.extractObject('user', item, buildUser);
      }).toThrow('Unexpected string data type for user');
    });

    test('should throw error for null value', () => {
      const item = { user: null, status: 'active' };
      expect(() => {
        ItemParser.extractObject('user', item, buildUser);
      }).toThrow('Expected object');
    });

    test('should throw error for array value', () => {
      const item = { user: ['not', 'an', 'object'], status: 'active' };
      expect(() => {
        ItemParser.extractObject('user', item, buildUser);
      }).toThrow('Expected object');
    });

    test('should throw error for undefined value', () => {
      const item = { status: 'active' };
      expect(() => {
        ItemParser.extractObject('user', item, buildUser);
      }).toThrow('Unexpected undefined data type for user');
    });

    test('should throw error when build function fails', () => {
      const item = {
        user: { id: 123, name: 'John Doe' }, // id should be string
        status: 'active',
      };
      expect(() => {
        ItemParser.extractObject('user', item, buildUser);
      }).toThrow('Unexpected number data type for id');
    });
  });

  describe('extractOptionalObject', () => {
    interface User {
      id: string;
      name: string;
    }

    const buildUser = (x: NativeAttributeValue): User => {
      if (typeof x === 'object' && x !== null && !Array.isArray(x)) {
        const obj = x as Record<string, NativeAttributeValue>;
        return {
          id: ItemParser.extractString('id', obj),
          name: ItemParser.extractString('name', obj),
        };
      }
      throw new Error('Expected object');
    };

    test('should extract object successfully', () => {
      const item = {
        user: { id: '123', name: 'John Doe' },
        status: 'active',
      };
      const result = ItemParser.extractOptionalObject('user', item, buildUser);
      expect(result).toEqual({ id: '123', name: 'John Doe' });
    });

    test('should return undefined for missing key', () => {
      const item = { status: 'active' };
      const result = ItemParser.extractOptionalObject('user', item, buildUser);
      expect(result).toBeUndefined();
    });

    test('should return undefined for undefined value', () => {
      const item = { user: undefined, status: 'active' };
      const result = ItemParser.extractOptionalObject('user', item, buildUser);
      expect(result).toBeUndefined();
    });

    test('should throw error for non-object value', () => {
      const item = { user: 'not-an-object', status: 'active' };
      expect(() => {
        ItemParser.extractOptionalObject('user', item, buildUser);
      }).toThrow('Unexpected string data type for user');
    });

    test('should throw error for null value', () => {
      const item = { user: null, status: 'active' };
      expect(() => {
        ItemParser.extractOptionalObject('user', item, buildUser);
      }).toThrow('Expected object');
    });

    test('should throw error for array value', () => {
      const item = { user: ['not', 'an', 'object'], status: 'active' };
      expect(() => {
        ItemParser.extractOptionalObject('user', item, buildUser);
      }).toThrow('Expected object');
    });

    test('should throw error when build function fails', () => {
      const item = {
        user: { id: 123, name: 'John Doe' }, // id should be string
        status: 'active',
      };
      expect(() => {
        ItemParser.extractOptionalObject('user', item, buildUser);
      }).toThrow('Unexpected number data type for id');
    });
  });
});
