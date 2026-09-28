import { InvalidDynamoDbPathError } from '../src';
import { pathKey, renderPath } from '../src/attribute-path';
import { AttributeNameSession } from '../src/attribute-session';

describe('attribute paths', () => {
  let names: AttributeNameSession;

  beforeEach(() => {
    names = new AttributeNameSession();
  });

  test('a name becomes a placeholder', () => {
    expect(renderPath('status', names)).toBe('#a0');
  });

  test('map keys join with dots and list indexes are written in brackets', () => {
    expect(renderPath(['notes', 3, 'noteId'], names)).toBe('#a0[3].#a1');
    expect(renderPath(['matrix', 0, 1], names)).toBe('#a2[0][1]');
    expect(names.expressionAttributeNames).toEqual({ '#a0': 'notes', '#a1': 'noteId', '#a2': 'matrix' });
  });

  test('pathKey names the location, not the placeholders', () => {
    expect(pathKey(['notes', 3, 'noteId'])).toBe('notes[3].noteId');
    expect(pathKey('status')).toBe('status');
  });

  test('refuses an empty path, a leading index and an index that is not a non-negative integer', () => {
    expect(() => renderPath([], names)).toThrow(InvalidDynamoDbPathError);
    expect(() => renderPath([0, 'a'], names)).toThrow('A path starts with an attribute name, not a list index.');
    expect(() => renderPath(['a', -1], names)).toThrow('A list index is a non-negative integer, got -1.');
    expect(() => pathKey(['a', 1.5])).toThrow(InvalidDynamoDbPathError);
  });
});
