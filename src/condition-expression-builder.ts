import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributeNameSession, AttributeValueSession } from './attribute-session';

export interface ConditionExpression {
  readonly expression: string;
}

/**
 * Full of the conditions are listed here. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax
 *
 * ToDo: support IN, contains, size, attribute_type
 */
interface _Condition {
  readonly type: 'attribute_exists' | 'attribute_not_exists' | '=' | '<' | '<=' | '>' | '>=' | 'between' | 'begins_with';
  readonly path: string | ReadonlyArray<string>;
}

interface OneOperandCondition extends _Condition {
  readonly type: 'attribute_exists' | 'attribute_not_exists';
}

interface TwoOperandsCondition extends _Condition {
  readonly type: '=' | '<' | '<=' | '>' | '>=' | 'begins_with';
  readonly value: NativeAttributeValue;
}

interface BetweenCondition extends _Condition {
  readonly type: 'between';
  readonly greaterThanOrEqualTo: NativeAttributeValue;
  readonly lessThanOrEqualTo: NativeAttributeValue;
}

type Condition = OneOperandCondition | TwoOperandsCondition | BetweenCondition;

export class ConditionExpressionBuilder {
  private readonly attributeNameSession: AttributeNameSession;
  private readonly attributeValueSession: AttributeValueSession;

  constructor(attributeNameSession?: AttributeNameSession, attributeValueSession?: AttributeValueSession) {
    this.attributeNameSession = attributeNameSession ?? new AttributeNameSession();
    this.attributeValueSession = attributeValueSession ?? new AttributeValueSession();
  }

  get expressionAttributeNames(): Record<string, string> {
    return this.attributeNameSession.expressionAttributeNames;
  }

  get expressionAttributeValues(): Record<string, string> {
    return this.attributeValueSession.expressionAttributeValues;
  }

  attributeExists(path: string | string[]): ConditionExpression {
    return this.condition({
      type: 'attribute_exists',
      path: path,
    });
  }

  attributeNotExists(path: string | string[]): ConditionExpression {
    return this.condition({
      type: 'attribute_not_exists',
      path: path,
    });
  }

  and(...conditions: ConditionExpression[]): ConditionExpression {
    return {
      expression: conditions.map((condition) => `(${condition.expression})`).join(' AND '),
    };
  }

  or(...conditions: ConditionExpression[]): ConditionExpression {
    return {
      expression: conditions.map((condition) => `(${condition.expression})`).join(' OR '),
    };
  }

  not(condition: ConditionExpression): ConditionExpression {
    return {
      expression: `NOT (${condition.expression})`,
    };
  }

  lessThan(path: string | ReadonlyArray<string>, value: string | number | boolean): ConditionExpression {
    return this.condition({
      type: '<',
      path: path,
      value: value,
    });
  }

  lessThanOrEqualTo(path: string | ReadonlyArray<string>, value: string | number | boolean): ConditionExpression {
    return this.condition({
      type: '<=',
      path: path,
      value: value,
    });
  }

  greaterThan(path: string | ReadonlyArray<string>, value: string | number | boolean): ConditionExpression {
    return this.condition({
      type: '>',
      path: path,
      value: value,
    });
  }

  greaterThanOrEqualTo(path: string | ReadonlyArray<string>, value: string | number | boolean): ConditionExpression {
    return this.condition({
      type: '>=',
      path: path,
      value: value,
    });
  }
  equal(path: string | ReadonlyArray<string>, value: string | number | boolean): ConditionExpression {
    return this.condition({
      type: '=',
      path: path,
      value: value,
    });
  }

  private condition(condition: Condition): ConditionExpression {
    let segments: ReadonlyArray<string>;
    if (typeof condition.path === 'string') {
      segments = [condition.path];
    } else {
      segments = condition.path;
    }

    const attributeNameIdentifiers: string[] = [];
    segments.forEach((segment) => {
      attributeNameIdentifiers.push(this.attributeNameSession.provideAttributeNameIdentifier(segment));
    });
    const attributeNameIdentifier = attributeNameIdentifiers.join('.');
    if (condition.type === 'attribute_exists' || condition.type === 'attribute_not_exists') {
      return {
        expression: `${condition.type}(${attributeNameIdentifier})`,
      };
    } else if (condition.type === 'between') {
      // two operands
      const greaterThanOrEqualToAttributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.greaterThanOrEqualTo);
      const lessThanOrEqualToAttributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.lessThanOrEqualTo);
      const expression = `${attributeNameIdentifier} BETWEEN ${greaterThanOrEqualToAttributeValueIdentifier} AND ${lessThanOrEqualToAttributeValueIdentifier}`;
      return {
        expression: expression,
      };
    } else if (condition.type === 'begins_with') {
      // two operands
      const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.value);
      const expression = `begins_with(${attributeNameIdentifier}, ${attributeValueIdentifier})`;

      return {
        expression: expression,
      };
    } else if (condition.type === '=' || condition.type === '<' || condition.type === '<=' || condition.type === '>' || condition.type === '>=') {
      // two operands
      const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.value);
      const expression = `${attributeNameIdentifier} ${condition.type} ${attributeValueIdentifier}`;
      return {
        expression: expression,
      };
    } else {
      throw new Error(`Unsupport condition operator ${condition.type}`);
    }
  }
}
