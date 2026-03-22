import { ConditionExpressionBuilder } from '../src';

describe('ConditionExpressionBuilder', () => {
  let cond: ConditionExpressionBuilder;

  beforeEach(() => {
    cond = new ConditionExpressionBuilder();
  });

  describe('comparison operators', () => {
    test('equal', () => {
      const result = cond.equal('status', 'active');
      expect(result.expression).toBe('#a0 = :v0');
      expect(cond.expressionAttributeNames).toEqual({ '#a0': 'status' });
      expect(cond.expressionAttributeValues).toEqual({ ':v0': 'active' });
    });

    test('lessThan', () => {
      const result = cond.lessThan('age', 30);
      expect(result.expression).toBe('#a0 < :v0');
    });

    test('lessThanOrEqualTo', () => {
      const result = cond.lessThanOrEqualTo('age', 30);
      expect(result.expression).toBe('#a0 <= :v0');
    });

    test('greaterThan', () => {
      const result = cond.greaterThan('age', 18);
      expect(result.expression).toBe('#a0 > :v0');
    });

    test('greaterThanOrEqualTo', () => {
      const result = cond.greaterThanOrEqualTo('score', 90);
      expect(result.expression).toBe('#a0 >= :v0');
    });

    test('nested path', () => {
      const result = cond.greaterThan(['order', 'total'], 1000);
      expect(result.expression).toBe('#a0.#a1 > :v0');
      expect(cond.expressionAttributeNames).toEqual({ '#a0': 'order', '#a1': 'total' });
    });

    test('boolean value', () => {
      const result = cond.equal('active', true);
      expect(cond.expressionAttributeValues).toEqual({ ':v0': true });
    });
  });

  describe('attribute_exists / attribute_not_exists', () => {
    test('attributeExists with string path', () => {
      const result = cond.attributeExists('email');
      expect(result.expression).toBe('attribute_exists(#a0)');
    });

    test('attributeExists with nested path', () => {
      const result = cond.attributeExists(['metadata', 'createdAt']);
      expect(result.expression).toBe('attribute_exists(#a0.#a1)');
    });

    test('attributeNotExists with string path', () => {
      const result = cond.attributeNotExists('deletedAt');
      expect(result.expression).toBe('attribute_not_exists(#a0)');
    });

    test('attributeNotExists with nested path', () => {
      const result = cond.attributeNotExists(['metadata', 'deletedAt']);
      expect(result.expression).toBe('attribute_not_exists(#a0.#a1)');
    });
  });

  describe('logical operators', () => {
    test('and', () => {
      const result = cond.and(cond.equal('a', 1), cond.equal('b', 2));
      expect(result.expression).toBe('(#a0 = :v0) AND (#a1 = :v1)');
    });

    test('or', () => {
      const result = cond.or(cond.equal('a', 1), cond.equal('b', 2));
      expect(result.expression).toBe('(#a0 = :v0) OR (#a1 = :v1)');
    });

    test('not', () => {
      const result = cond.not(cond.equal('status', 'deleted'));
      expect(result.expression).toBe('NOT (#a0 = :v0)');
    });

    test('complex nested logic', () => {
      const result = cond.or(
        cond.equal('accountId', '1234'),
        cond.and(
          cond.greaterThan('order.total', 1000),
          cond.greaterThan(['order', 'total'], 1000),
        ),
      );
      expect(result.expression).toBe('(#a0 = :v0) OR ((#a1 > :v1) AND (#a2.#a3 > :v2))');
      // 'order.total' as a single string is a different attribute name than ['order', 'total']
      expect(cond.expressionAttributeNames['#a1']).toBe('order.total');
      expect(cond.expressionAttributeNames['#a2']).toBe('order');
      expect(cond.expressionAttributeNames['#a3']).toBe('total');
    });

    test('and with three conditions', () => {
      const result = cond.and(
        cond.attributeExists('a'),
        cond.equal('b', 1),
        cond.greaterThan('c', 0),
      );
      expect(result.expression).toBe('(attribute_exists(#a0)) AND (#a1 = :v0) AND (#a2 > :v1)');
    });
  });

  describe('standalone usage (own attribute sessions)', () => {
    test('expressionAttributeNames and expressionAttributeValues are accessible', () => {
      cond.equal('version', 5);
      expect(cond.expressionAttributeNames).toEqual({ '#a0': 'version' });
      expect(cond.expressionAttributeValues).toEqual({ ':v0': 5 });
    });

    test('reuses attribute name identifiers for same name', () => {
      cond.and(cond.attributeExists('version'), cond.equal('version', 5));
      // 'version' should be #a0 in both
      expect(cond.expressionAttributeNames).toEqual({ '#a0': 'version' });
    });

    test('throws when adding condition after expressionAttributeValues is accessed', () => {
      cond.equal('a', 1);
      // Only finalize the value session, not the name session
      cond.expressionAttributeValues;
      // attributeExists only uses name session — should still work
      expect(() => cond.attributeExists('b')).not.toThrow();
      // equal uses name session first (succeeds since not finalized), then value session (throws)
      expect(() => cond.equal('c', 2)).toThrow();
    });
  });
});
