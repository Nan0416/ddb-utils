import { AttributeSessionFinalizedError, UpdateExpression, UpdateExpressionBuilder } from '../src';

describe('update-expression-builder-test', () => {
  const ACCOUNT_ID = 'mockAccountId';
  const LAST_UPDATED_AT = 'mockUpdatedAt';
  const NEW_VERSION = 'newVersion';
  const OLD_VERSION = 'oldVersion';
  const DATA = { k1: 'v1', k2: 'v2' };

  let updateExpressionBuilder: UpdateExpressionBuilder;

  beforeEach(() => {
    updateExpressionBuilder = new UpdateExpressionBuilder();
  });

  test('update two properties', () => {
    const expression = updateExpressionBuilder.set('accountId', ACCOUNT_ID).set('lastUpdatedAt', LAST_UPDATED_AT).build();
    const expectedUpdateExpression: UpdateExpression = {
      updateExpression: 'SET #a0 = :v0, #a1 = :v1',
      expressionAttributeValues: {
        ':v0': ACCOUNT_ID,
        ':v1': LAST_UPDATED_AT,
      },
      expressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'lastUpdatedAt',
      },
    };
    expect(expression).toEqual(expectedUpdateExpression);
  });

  test('update two properties and delete one property', () => {
    const expression = updateExpressionBuilder.set('accountId', ACCOUNT_ID).set('lastUpdatedAt', LAST_UPDATED_AT).delete('name').build();
    const expectedUpdateExpression: UpdateExpression = {
      updateExpression: 'SET #a0 = :v0, #a1 = :v1 REMOVE #a2',
      expressionAttributeValues: {
        ':v0': ACCOUNT_ID,
        ':v1': LAST_UPDATED_AT,
      },
      expressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'lastUpdatedAt',
        '#a2': 'name',
      },
    };
    expect(expression).toEqual(expectedUpdateExpression);
  });

  test('update two properties with condition', () => {
    updateExpressionBuilder.set('accountId', ACCOUNT_ID).set('lastUpdatedAt', LAST_UPDATED_AT).set('version', NEW_VERSION);
    const conditional = updateExpressionBuilder.conditionExpressionBuilder;
    const condition = conditional.and(conditional.attribute_exists('accountId'), conditional.equal('version', OLD_VERSION));

    const expression = updateExpressionBuilder.build();

    const expectedUpdateExpression: UpdateExpression = {
      updateExpression: 'SET #a0 = :v0, #a1 = :v1, #a2 = :v2',
      expressionAttributeValues: {
        ':v0': ACCOUNT_ID,
        ':v1': LAST_UPDATED_AT,
        ':v2': NEW_VERSION,
        ':v3': OLD_VERSION,
      },
      expressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'lastUpdatedAt',
        '#a2': 'version',
      },
    };
    expect(expression).toEqual(expectedUpdateExpression);
    expect(condition.expression).toEqual('(attribute_exists(#a0)) AND (#a2 = :v3)');
  });

  test('it should throw error when updating condition after update expression is built.', () => {
    updateExpressionBuilder.set('accountId', ACCOUNT_ID).set('lastUpdatedAt', LAST_UPDATED_AT).set('version', NEW_VERSION).build();

    expect(() => {
      const conditional = updateExpressionBuilder.conditionExpressionBuilder;
      conditional.and(conditional.attribute_exists('accountId'), conditional.equal('version', OLD_VERSION));
    }).toThrow(AttributeSessionFinalizedError);
  });

  test('with nested data structure', () => {
    updateExpressionBuilder.set('accountId', ACCOUNT_ID).set(['timestamp', 'lastUpdatedAt'], LAST_UPDATED_AT).set('data', DATA);
    const conditional = updateExpressionBuilder.conditionExpressionBuilder;
    const condition = conditional.and(conditional.attribute_exists('accountId'), conditional.attribute_exists(['timestamp', 'createdAt']));
    const expression = updateExpressionBuilder.build();

    const expectedUpdateExpression: UpdateExpression = {
      updateExpression: 'SET #a0 = :v0, #a1.#a2 = :v1, #a3 = :v2',
      expressionAttributeValues: {
        ':v0': ACCOUNT_ID,
        ':v1': LAST_UPDATED_AT,
        ':v2': DATA,
      },
      expressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'timestamp',
        '#a2': 'lastUpdatedAt',
        '#a3': 'data',
        '#a4': 'createdAt',
      },
    };
    expect(expression).toEqual(expectedUpdateExpression);
    expect(condition.expression).toEqual('(attribute_exists(#a0)) AND (attribute_exists(#a1.#a4))');
  });

  test('ss', () => {
    const updateExpressionBuilder = new UpdateExpressionBuilder();

    updateExpressionBuilder.set('active', true).set('lastUpdatedAt', '2025-04-28T10:00:00Z').set('version', 'qoe38');
    const conditional = updateExpressionBuilder.conditionExpressionBuilder;
    const condition = conditional.and(conditional.attribute_exists('accountId'), conditional.equal('version', 'j893w'));
    const expression = updateExpressionBuilder.build();

    console.log(JSON.stringify(expression, null, 2));
    console.log(condition.expression);
  });
});
