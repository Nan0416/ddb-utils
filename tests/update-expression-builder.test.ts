import { AttributeSessionFinalizedError, InvalidDynamoDbUpdateRequestError, UpdateExpression, UpdateExpressionBuilder } from '../src';

describe('UpdateExpressionBuilder', () => {
  let builder: UpdateExpressionBuilder;

  beforeEach(() => {
    builder = new UpdateExpressionBuilder();
  });

  describe('set', () => {
    test('single property', () => {
      const expression = builder.set('accountId', 'abc123').build();
      expect(expression).toEqual({
        updateExpression: 'SET #a0 = :v0',
        expressionAttributeValues: { ':v0': 'abc123' },
        expressionAttributeNames: { '#a0': 'accountId' },
      });
    });

    test('multiple properties', () => {
      const expression = builder.set('accountId', 'abc123').set('lastUpdatedAt', '2025-01-01').build();
      expect(expression).toEqual({
        updateExpression: 'SET #a0 = :v0, #a1 = :v1',
        expressionAttributeValues: { ':v0': 'abc123', ':v1': '2025-01-01' },
        expressionAttributeNames: { '#a0': 'accountId', '#a1': 'lastUpdatedAt' },
      });
    });

    test('nested path as array', () => {
      const expression = builder.set(['timestamp', 'lastUpdatedAt'], '2025-01-01').build();
      expect(expression).toEqual({
        updateExpression: 'SET #a0.#a1 = :v0',
        expressionAttributeValues: { ':v0': '2025-01-01' },
        expressionAttributeNames: { '#a0': 'timestamp', '#a1': 'lastUpdatedAt' },
      });
    });

    test('object value', () => {
      const data = { k1: 'v1', k2: 'v2' };
      const expression = builder.set('data', data).build();
      expect(expression.expressionAttributeValues[':v0']).toEqual(data);
    });

    test('boolean value', () => {
      const expression = builder.set('active', true).build();
      expect(expression.expressionAttributeValues[':v0']).toBe(true);
    });

    test('number value', () => {
      const expression = builder.set('count', 42).build();
      expect(expression.expressionAttributeValues[':v0']).toBe(42);
    });
  });

  describe('delete (REMOVE)', () => {
    test('single property', () => {
      const expression = builder.delete('name').build();
      expect(expression).toEqual({
        updateExpression: 'REMOVE #a0',
        expressionAttributeValues: {},
        expressionAttributeNames: { '#a0': 'name' },
      });
    });

    test('multiple properties', () => {
      const expression = builder.delete('name').delete('email').build();
      expect(expression).toEqual({
        updateExpression: 'REMOVE #a0, #a1',
        expressionAttributeValues: {},
        expressionAttributeNames: { '#a0': 'name', '#a1': 'email' },
      });
    });

    test('nested path', () => {
      const expression = builder.delete(['metadata', 'tags']).build();
      expect(expression).toEqual({
        updateExpression: 'REMOVE #a0.#a1',
        expressionAttributeValues: {},
        expressionAttributeNames: { '#a0': 'metadata', '#a1': 'tags' },
      });
    });
  });

  describe('SET + REMOVE combined', () => {
    test('set and delete in one expression', () => {
      const expression = builder.set('accountId', 'abc123').set('lastUpdatedAt', '2025-01-01').delete('name').build();
      expect(expression.updateExpression).toBe('SET #a0 = :v0, #a1 = :v1 REMOVE #a2');
    });
  });

  describe('addToSet (ADD)', () => {
    test('add string set values', () => {
      const expression = builder.addToSet('tags', new Set(['tag1', 'tag2'])).build();
      expect(expression).toEqual({
        updateExpression: 'ADD #a0 :v0',
        expressionAttributeValues: { ':v0': new Set(['tag1', 'tag2']) },
        expressionAttributeNames: { '#a0': 'tags' },
      });
    });

    test('add number set values', () => {
      const expression = builder.addToSet('scores', new Set([1, 2, 3])).build();
      expect(expression).toEqual({
        updateExpression: 'ADD #a0 :v0',
        expressionAttributeValues: { ':v0': new Set([1, 2, 3]) },
        expressionAttributeNames: { '#a0': 'scores' },
      });
    });

    test('increment a number', () => {
      const expression = builder.addToSet('viewCount', 1).build();
      expect(expression).toEqual({
        updateExpression: 'ADD #a0 :v0',
        expressionAttributeValues: { ':v0': 1 },
        expressionAttributeNames: { '#a0': 'viewCount' },
      });
    });

    test('multiple add operations', () => {
      const expression = builder.addToSet('tags', new Set(['tag1'])).addToSet('scores', new Set([100])).build();
      expect(expression.updateExpression).toBe('ADD #a0 :v0, #a1 :v1');
    });

    test('nested path', () => {
      const expression = builder.addToSet(['metadata', 'tags'], new Set(['tag1'])).build();
      expect(expression.updateExpression).toBe('ADD #a0.#a1 :v0');
    });
  });

  describe('deleteFromSet (DELETE)', () => {
    test('delete string set values', () => {
      const expression = builder.deleteFromSet('tags', new Set(['tag1', 'tag2'])).build();
      expect(expression).toEqual({
        updateExpression: 'DELETE #a0 :v0',
        expressionAttributeValues: { ':v0': new Set(['tag1', 'tag2']) },
        expressionAttributeNames: { '#a0': 'tags' },
      });
    });

    test('multiple delete operations', () => {
      const expression = builder.deleteFromSet('tags', new Set(['tag1'])).deleteFromSet('scores', new Set([100])).build();
      expect(expression.updateExpression).toBe('DELETE #a0 :v0, #a1 :v1');
    });

    test('nested path', () => {
      const expression = builder.deleteFromSet(['metadata', 'tags'], new Set(['tag1'])).build();
      expect(expression.updateExpression).toBe('DELETE #a0.#a1 :v0');
    });
  });

  describe('all four actions combined', () => {
    test('SET + REMOVE + ADD + DELETE in one expression', () => {
      const expression = builder
        .set('version', 2)
        .delete('deprecated')
        .addToSet('connectionIds', new Set(['c1']))
        .deleteFromSet('oldIds', new Set(['c0']))
        .build();

      expect(expression.updateExpression).toBe('SET #a0 = :v0 REMOVE #a1 ADD #a2 :v1 DELETE #a3 :v2');
      expect(expression.expressionAttributeNames).toEqual({
        '#a0': 'version',
        '#a1': 'deprecated',
        '#a2': 'connectionIds',
        '#a3': 'oldIds',
      });
      expect(expression.expressionAttributeValues).toEqual({
        ':v0': 2,
        ':v1': new Set(['c1']),
        ':v2': new Set(['c0']),
      });
    });

    test('ADD + SET with optimistic locking condition', () => {
      builder.addToSet('connectionIds', new Set(['c1'])).set('version', 6);
      const cond = builder.conditionExpressionBuilder;
      const condition = cond.equal('version', 5);
      const expression = builder.build();

      expect(expression.updateExpression).toBe('SET #a1 = :v1 ADD #a0 :v0');
      expect(condition.expression).toBe('#a1 = :v2');
      expect(expression.expressionAttributeValues[':v2']).toBe(5);
    });
  });

  describe('append (list_append)', () => {
    test('append to end (default)', () => {
      const expression = builder.append('list', { key: 'value' }).build();
      expect(expression.updateExpression).toBe('SET #a0 = list_append(if_not_exists(#a0, :v1), :v0)');
      expect(expression.expressionAttributeValues[':v0']).toEqual([{ key: 'value' }]);
      expect(expression.expressionAttributeValues[':v1']).toEqual([]);
    });

    test('append to start', () => {
      const expression = builder.append('list', 'item', 'start').build();
      expect(expression.updateExpression).toBe('SET #a0 = list_append(:v0, if_not_exists(#a0, :v1))');
    });

    test('append with failIfMissing', () => {
      const expression = builder.append('list', 'item', 'end', true).build();
      // No if_not_exists wrapper when failIfMissing is true
      expect(expression.updateExpression).toBe('SET #a0 = list_append(#a0, :v0)');
    });

    test('append array value directly', () => {
      const expression = builder.append('list', ['a', 'b']).build();
      // Array values are passed through as-is
      expect(expression.expressionAttributeValues[':v0']).toEqual(['a', 'b']);
    });
  });

  describe('conditionExpressionBuilder', () => {
    test('shares attribute sessions with update builder', () => {
      builder.set('accountId', 'abc').set('version', 'new');
      const cond = builder.conditionExpressionBuilder;
      const condition = cond.and(cond.attributeExists('accountId'), cond.equal('version', 'old'));
      const expression = builder.build();

      // accountId reuses #a0, version reuses #a2
      expect(condition.expression).toBe('(attribute_exists(#a0)) AND (#a1 = :v2)');
      expect(expression.expressionAttributeValues[':v2']).toBe('old');
    });

    test('throws after build is called', () => {
      builder.set('x', 1).build();
      expect(() => {
        builder.conditionExpressionBuilder.attributeExists('y');
      }).toThrow(AttributeSessionFinalizedError);
    });

    test('throws when adding condition value after build', () => {
      builder.set('x', 1).build();
      // After build, both sessions are finalized — the name session throws first
      // since equal() calls provideAttributeNameIdentifier before provideAttributeValueIdentifier.
      // The value session finalization is tested in the condition-expression-builder tests.
      expect(() => {
        builder.conditionExpressionBuilder.equal('y', 2);
      }).toThrow(AttributeSessionFinalizedError);
    });
  });

  describe('hasUpdate', () => {
    test('returns false for empty builder', () => {
      expect(builder.hasUpdate()).toBe(false);
    });

    test('returns true for set', () => {
      builder.set('x', 1);
      expect(builder.hasUpdate()).toBe(true);
    });

    test('returns true for delete', () => {
      builder.delete('x');
      expect(builder.hasUpdate()).toBe(true);
    });

    test('returns true for addToSet', () => {
      builder.addToSet('x', new Set(['a']));
      expect(builder.hasUpdate()).toBe(true);
    });

    test('returns true for deleteFromSet', () => {
      builder.deleteFromSet('x', new Set(['a']));
      expect(builder.hasUpdate()).toBe(true);
    });
  });

  describe('error handling', () => {
    test('throws on empty build', () => {
      expect(() => builder.build()).toThrow(InvalidDynamoDbUpdateRequestError);
    });

    test('throws on duplicate path', () => {
      builder.set('accountId', 'abc');
      expect(() => builder.set('accountId', 'def')).toThrow(InvalidDynamoDbUpdateRequestError);
      expect(() => builder.set('accountId', 'def')).toThrow('Path accountId is already in the update list.');
    });

    test('throws on duplicate nested path', () => {
      builder.set(['a', 'b'], 1);
      expect(() => builder.set(['a', 'b'], 2)).toThrow(InvalidDynamoDbUpdateRequestError);
    });
  });
});
