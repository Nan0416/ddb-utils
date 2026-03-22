import { InvalidDynamoDbProjectionRequestError, ProjectionExpressionBuilder } from '../src';

describe('ProjectionExpressionBuilder', () => {
  let builder: ProjectionExpressionBuilder;

  beforeEach(() => {
    builder = new ProjectionExpressionBuilder();
  });

  test('single field', () => {
    const expression = builder.get('accountId').build();
    expect(expression).toEqual({
      projectionExpression: '#a0',
      expressionAttributeNames: { '#a0': 'accountId' },
    });
  });

  test('nested path', () => {
    const expression = builder.get('accountId').get(['order', 'urfi3u']).get(['order', 'du9834']).build();
    expect(expression).toEqual({
      projectionExpression: '#a0, #a1.#a2, #a1.#a3',
      expressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'order',
        '#a2': 'urfi3u',
        '#a3': 'du9834',
      },
    });
  });

  test('string path is treated as single segment', () => {
    const expression = builder.get('name').build();
    expect(expression.projectionExpression).toBe('#a0');
  });

  test('duplicate path is deduplicated', () => {
    const expression = builder.get('accountId').get('accountId').build();
    expect(expression.projectionExpression).toBe('#a0');
  });

  test('duplicate nested path is deduplicated', () => {
    const expression = builder.get(['a', 'b']).get(['a', 'b']).build();
    expect(expression.projectionExpression).toBe('#a0.#a1');
  });

  test('hasProjection returns false when empty', () => {
    expect(builder.hasProjection()).toBe(false);
  });

  test('hasProjection returns true after get', () => {
    builder.get('x');
    expect(builder.hasProjection()).toBe(true);
  });

  test('throws on empty build', () => {
    expect(() => builder.build()).toThrow(InvalidDynamoDbProjectionRequestError);
  });
});
