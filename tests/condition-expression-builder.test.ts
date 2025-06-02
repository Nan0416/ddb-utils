import { ConditionExpressionBuilder } from '../src';

describe('conditional-expression-builder-test', () => {
  let conditional: ConditionExpressionBuilder;

  beforeEach(() => {
    conditional = new ConditionExpressionBuilder();
  });

  test('condition with nested data', () => {
    const condition = conditional.greaterThan(['order', 'du9834'], 1000);
    const expectedConditionExpression = {
      ConditionExpression: '#a0.#a1 > :v0',
      ExpressionAttributeNames: {
        '#a0': 'order',
        '#a1': 'du9834',
      },
      ExpressionAttributeValues: {
        ':v0': 1000,
      },
    };
    expect({
      ConditionExpression: condition.expression,
      ExpressionAttributeNames: conditional.expressionAttributeNames,
      ExpressionAttributeValues: conditional.expressionAttributeValues,
    }).toEqual(expectedConditionExpression);
  });

  test('projection with nested data', () => {
    const condition = conditional.or(conditional.equal('accountId', '1234'), conditional.and(conditional.greaterThan('order.du9834', 1000), conditional.greaterThan(['order', 'du9834'], 1000)));
    const expectedConditionExpression = {
      ConditionExpression: '(#a0 = :v0) OR ((#a1 > :v1) AND (#a2.#a3 > :v2))',
      ExpressionAttributeNames: {
        '#a0': 'accountId',
        '#a1': 'order.du9834',
        '#a2': 'order',
        '#a3': 'du9834',
      },
      ExpressionAttributeValues: {
        ':v0': '1234',
        ':v1': 1000,
        ':v2': 1000,
      },
    };
    expect({
      ConditionExpression: condition.expression,
      ExpressionAttributeNames: conditional.expressionAttributeNames,
      ExpressionAttributeValues: conditional.expressionAttributeValues,
    }).toEqual(expectedConditionExpression);
  });
});
