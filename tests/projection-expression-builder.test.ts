import { InvalidDynamoDbProjectionRequestError, ProjectionExpression, ProjectionExpressionBuilder, UpdateExpression, UpdateExpressionBuilder } from '../src';

describe('update-expression-builder-test', () => {
  const ACCOUNT_ID = 'mockAccountId';
  const LAST_UPDATED_AT = 'mockUpdatedAt';
  const NEW_VERSION = 'newVersion';
  const OLD_VERSION = 'oldVersion';
  const DATA = { k1: 'v1', k2: 'v2' };

  let projectionExpressionBuilder: ProjectionExpressionBuilder;

  beforeEach(() => {
    projectionExpressionBuilder = new ProjectionExpressionBuilder();
  });

  test('projection with nested data', () => {
    const expression = projectionExpressionBuilder.get('accountId').get(['order', 'urfi3u']).get(['order', 'du9834']).build();
    const expectedUpdateExpression: ProjectionExpression = {
      projectionExpression: '#a0, #a1.#a2, #a1.#a3',
      expressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'order',
        '#a2': 'urfi3u',
        '#a3': 'du9834',
      },
    };
    expect(expression).toEqual(expectedUpdateExpression);
  });

  test('build without projection, it should throw error', () => {
    expect.hasAssertions();
    try {
      projectionExpressionBuilder.build();
    } catch (err: any) {
      expect(err instanceof InvalidDynamoDbProjectionRequestError).toBeTruthy();
    }
  });
});
