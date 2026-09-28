import { AttributeNameSession } from './attribute-session';
import { InvalidDynamoDbPathError } from './errors';

/**
 * A document path: an attribute name, or its segments from the top-level attribute down. A
 * string segment is a map key and a number is a list index: ['notes', 3, 'noteId'] is
 * `notes[3].noteId`. Names become placeholders; an index is written into the expression,
 * because DynamoDB does not accept a placeholder for one.
 */
export type AttributePath = string | ReadonlyArray<string | number>;

function segmentsOf(path: AttributePath): ReadonlyArray<string | number> {
  const segments = typeof path === 'string' ? [path] : path;
  if (segments.length === 0) {
    throw new InvalidDynamoDbPathError('A path needs at least one segment.');
  }
  if (typeof segments[0] !== 'string') {
    throw new InvalidDynamoDbPathError('A path starts with an attribute name, not a list index.');
  }
  segments.forEach((segment) => {
    if (typeof segment === 'number' && (!Number.isInteger(segment) || segment < 0)) {
      throw new InvalidDynamoDbPathError(`A list index is a non-negative integer, got ${segment}.`);
    }
  });
  return segments;
}

/** The path as the expression writes it, with a placeholder for each name. */
export function renderPath(path: AttributePath, attributeNameSession: AttributeNameSession): string {
  return segmentsOf(path)
    .map((segment, i) => (typeof segment === 'number' ? `[${segment}]` : `${i === 0 ? '' : '.'}${attributeNameSession.provideAttributeNameIdentifier(segment)}`))
    .join('');
}

/**
 * A key that is equal only for two paths naming the same document location. The segments
 * are serialized as JSON because an attribute name may itself contain `.` or `[`: 'a.b' and
 * ['a', 'b'], or 'a[0]' and ['a', 0], are different locations.
 */
export function pathKey(path: AttributePath): string {
  return JSON.stringify(segmentsOf(path));
}

/** The path as a reader would write it, for error messages: ['notes', 3, 'noteId'] is notes[3].noteId. */
export function describePath(path: AttributePath): string {
  return segmentsOf(path)
    .map((segment, i) => (typeof segment === 'number' ? `[${segment}]` : `${i === 0 ? '' : '.'}${segment}`))
    .join('');
}
