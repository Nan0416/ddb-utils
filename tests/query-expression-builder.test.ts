import { InvalidDynamoDbQueryRequestError, QueryConditionConflictError, QueryExpressionBuilder } from '../src';

describe('query-expression-builder-test', () => {
  let queryExpressionBuilder: QueryExpressionBuilder;

  beforeEach(() => {
    queryExpressionBuilder = new QueryExpressionBuilder();
  });

  test('should build basic query with partition key', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0',
      expressionAttributeNames: {
        '#a0': 'userId',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
      },
    });
  });

  test('should build query with partition key and equals sort key', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').equal('timestamp', 1234567890).build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0 AND #a1 = :v1',
      expressionAttributeNames: {
        '#a0': 'userId',
        '#a1': 'timestamp',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
        ':v1': 1234567890,
      },
    });
  });

  test('should build query with partition key and less than sort key', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').lessThan('timestamp', 1234567890).build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0 AND #a1 < :v1',
      expressionAttributeNames: {
        '#a0': 'userId',
        '#a1': 'timestamp',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
        ':v1': 1234567890,
      },
    });
  });

  test('should build query with partition key and greater than sort key', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').greaterThan('timestamp', 1234567890).build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0 AND #a1 > :v1',
      expressionAttributeNames: {
        '#a0': 'userId',
        '#a1': 'timestamp',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
        ':v1': 1234567890,
      },
    });
  });

  test('should build query with partition key and between sort key', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').between('timestamp', 1234567890, 9876543210).build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0 AND (#a1 BETWEEN :v1 AND :v2)',
      expressionAttributeNames: {
        '#a0': 'userId',
        '#a1': 'timestamp',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
        ':v1': 1234567890,
        ':v2': 9876543210,
      },
    });
  });

  test('should build query with partition key and begins_with sort key', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').beginsWith('timestamp', '2024').build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0 AND begins_with(#a1, :v1)',
      expressionAttributeNames: {
        '#a0': 'userId',
        '#a1': 'timestamp',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
        ':v1': '2024',
      },
    });
  });

  test('should build query with projection', () => {
    const expression = queryExpressionBuilder.key('userId', 'user123').equal('timestamp', 1234567890).project(['data', 'status']).build();
    expect(expression).toEqual({
      keyConditionExpression: '#a0 = :v0 AND #a1 = :v1',
      projectionExpression: '#a2.#a3',
      expressionAttributeNames: {
        '#a0': 'userId',
        '#a1': 'timestamp',
        '#a2': 'data',
        '#a3': 'status',
      },
      expressionAttributeValues: {
        ':v0': 'user123',
        ':v1': 1234567890,
      },
    });
  });

  test('should throw error when building without partition key', () => {
    expect(() => {
      queryExpressionBuilder.build();
    }).toThrow(InvalidDynamoDbQueryRequestError);
  });

  test('should throw error when setting partition key twice', () => {
    expect(() => {
      queryExpressionBuilder.key('userId', 'user123').key('timestamp', 'user456');
    }).toThrow(QueryConditionConflictError);
  });

  test('should throw error when setting sort key condition twice', () => {
    expect(() => {
      queryExpressionBuilder.key('userId', 'user123').equal('timestamp', 123).equal('timestamp', 456);
    }).toThrow(QueryConditionConflictError);
  });
});
