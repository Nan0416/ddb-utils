import { InvalidDynamoDbQueryRequestError, QueryConditionConflictError, QueryExpressionBuilder } from '../src';

describe('QueryExpressionBuilder', () => {
  let builder: QueryExpressionBuilder;

  beforeEach(() => {
    builder = new QueryExpressionBuilder();
  });

  describe('partition key only', () => {
    test('basic query', () => {
      const expression = builder.key('userId', 'user123').build();
      expect(expression).toEqual({
        keyConditionExpression: '#a0 = :v0',
        expressionAttributeNames: { '#a0': 'userId' },
        expressionAttributeValues: { ':v0': 'user123' },
      });
    });
  });

  describe('sort key conditions', () => {
    test('equal', () => {
      const expression = builder.key('userId', 'user123').equal('timestamp', 1234567890).build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND #a1 = :v1');
    });

    test('lessThan', () => {
      const expression = builder.key('userId', 'user123').lessThan('timestamp', 1234567890).build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND #a1 < :v1');
    });

    test('lessThanOrEqualTo', () => {
      const expression = builder.key('userId', 'user123').lessThanOrEqualTo('timestamp', 1234567890).build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND #a1 <= :v1');
    });

    test('greaterThan', () => {
      const expression = builder.key('userId', 'user123').greaterThan('timestamp', 1234567890).build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND #a1 > :v1');
    });

    test('greaterThanOrEqualTo', () => {
      const expression = builder.key('userId', 'user123').greaterThanOrEqualTo('timestamp', 1234567890).build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND #a1 >= :v1');
    });

    test('beginsWith', () => {
      const expression = builder.key('userId', 'user123').beginsWith('sk', 'p#').build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND begins_with(#a1, :v1)');
    });

    test('between', () => {
      const expression = builder.key('userId', 'user123').between('timestamp', 100, 200).build();
      expect(expression.keyConditionExpression).toBe('#a0 = :v0 AND (#a1 BETWEEN :v1 AND :v2)');
      expect(expression.expressionAttributeValues).toEqual({ ':v0': 'user123', ':v1': 100, ':v2': 200 });
    });
  });

  describe('projection', () => {
    test('single field', () => {
      const expression = builder.key('pk', 'val').project('name').build();
      expect(expression.projectionExpression).toBe('#a1');
    });

    test('nested field', () => {
      const expression = builder.key('pk', 'val').project(['data', 'status']).build();
      expect(expression.projectionExpression).toBe('#a1.#a2');
    });

    test('multiple fields', () => {
      const expression = builder.key('pk', 'val').project('name').project('age').build();
      expect(expression.projectionExpression).toBe('#a1, #a2');
    });

    test('no projection returns undefined', () => {
      const expression = builder.key('pk', 'val').build();
      expect(expression.projectionExpression).toBeUndefined();
    });
  });

  describe('filterExpressionBuilder', () => {
    test('filter expression shares attribute sessions', () => {
      builder.key('userId', 'user123');
      const filter = builder.filterExpressionBuilder;
      const condition = filter.equal('status', 'active');
      const expression = builder.build();

      expect(condition.expression).toBe('#a1 = :v1');
      expect(expression.expressionAttributeNames['#a1']).toBe('status');
      expect(expression.expressionAttributeValues[':v1']).toBe('active');
    });
  });

  describe('error handling', () => {
    test('throws without partition key', () => {
      expect(() => builder.build()).toThrow(InvalidDynamoDbQueryRequestError);
    });

    test('throws on duplicate partition key', () => {
      expect(() => builder.key('pk', 'a').key('pk', 'b')).toThrow(QueryConditionConflictError);
    });

    test('throws on duplicate sort key condition', () => {
      expect(() => builder.key('pk', 'a').equal('sk', 1).equal('sk', 2)).toThrow(QueryConditionConflictError);
    });
  });
});
