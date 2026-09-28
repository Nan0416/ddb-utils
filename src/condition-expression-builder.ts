import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributeNameSession, AttributeValueSession } from './attribute-session';
import { InvalidDynamoDbConditionRequestError } from './errors';

export interface ConditionExpression {
  readonly expression: string;
}

/** DynamoDB accepts at most 100 operands on the right of IN. */
const MAX_IN_OPERANDS = 100;

/**
 * Full of the conditions are listed here. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax
 *
 * ToDo: support size, attribute_type
 */
interface _Condition {
  readonly type: 'attribute_exists' | 'attribute_not_exists' | '=' | '<>' | '<' | '<=' | '>' | '>=' | 'between' | 'begins_with' | 'contains' | 'in';
  readonly path: string | ReadonlyArray<string>;
}

interface OneOperandCondition extends _Condition {
  readonly type: 'attribute_exists' | 'attribute_not_exists';
}

interface TwoOperandsCondition extends _Condition {
  readonly type: '=' | '<>' | '<' | '<=' | '>' | '>=' | 'begins_with' | 'contains';
  readonly value: NativeAttributeValue;
}

interface BetweenCondition extends _Condition {
  readonly type: 'between';
  readonly greaterThanOrEqualTo: NativeAttributeValue;
  readonly lessThanOrEqualTo: NativeAttributeValue;
}

interface InCondition extends _Condition {
  readonly type: 'in';
  readonly values: ReadonlyArray<NativeAttributeValue>;
}

type Condition = OneOperandCondition | TwoOperandsCondition | BetweenCondition | InCondition;

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

  get expressionAttributeValues(): Record<string, NativeAttributeValue> {
    return this.attributeValueSession.expressionAttributeValues;
  }

  attributeExists(path: string | ReadonlyArray<string>): ConditionExpression {
    return this.condition({
      type: 'attribute_exists',
      path: path,
    });
  }

  attributeNotExists(path: string | ReadonlyArray<string>): ConditionExpression {
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

  lessThan(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: '<',
      path: path,
      value: value,
    });
  }

  lessThanOrEqualTo(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: '<=',
      path: path,
      value: value,
    });
  }

  greaterThan(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: '>',
      path: path,
      value: value,
    });
  }

  greaterThanOrEqualTo(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: '>=',
      path: path,
      value: value,
    });
  }

  equal(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: '=',
      path: path,
      value: value,
    });
  }

  /** Also true when the attribute is absent: a missing attribute equals no value. */
  notEqual(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: '<>',
      path: path,
      value: value,
    });
  }

  /**
   * @param left inclusive
   * @param right inclusive
   */
  between(path: string | ReadonlyArray<string>, left: NativeAttributeValue, right: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: 'between',
      path: path,
      greaterThanOrEqualTo: left,
      lessThanOrEqualTo: right,
    });
  }

  beginsWith(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: 'begins_with',
      path: path,
      value: value,
    });
  }

  /** A substring of a string attribute, or a member of a set or list attribute. */
  contains(path: string | ReadonlyArray<string>, value: NativeAttributeValue): ConditionExpression {
    return this.condition({
      type: 'contains',
      path: path,
      value: value,
    });
  }

  /**
   * The attribute equals one of `values`.
   * @throws InvalidDynamoDbConditionRequestError for no values or more than 100, which DynamoDB rejects.
   */
  in(path: string | ReadonlyArray<string>, values: ReadonlyArray<NativeAttributeValue>): ConditionExpression {
    if (values.length === 0 || values.length > MAX_IN_OPERANDS) {
      throw new InvalidDynamoDbConditionRequestError(`IN takes 1 to ${MAX_IN_OPERANDS} values, got ${values.length}.`);
    }
    return this.condition({
      type: 'in',
      path: path,
      values: values,
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
      const greaterThanOrEqualToAttributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.greaterThanOrEqualTo);
      const lessThanOrEqualToAttributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.lessThanOrEqualTo);
      return {
        expression: `${attributeNameIdentifier} BETWEEN ${greaterThanOrEqualToAttributeValueIdentifier} AND ${lessThanOrEqualToAttributeValueIdentifier}`,
      };
    } else if (condition.type === 'in') {
      const attributeValueIdentifiers = condition.values.map((value) => this.attributeValueSession.provideAttributeValueIdentifier(value));
      return {
        expression: `${attributeNameIdentifier} IN (${attributeValueIdentifiers.join(', ')})`,
      };
    } else if (condition.type === 'begins_with' || condition.type === 'contains') {
      const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.value);
      return {
        expression: `${condition.type}(${attributeNameIdentifier}, ${attributeValueIdentifier})`,
      };
    } else if (condition.type === '=' || condition.type === '<>' || condition.type === '<' || condition.type === '<=' || condition.type === '>' || condition.type === '>=') {
      const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(condition.value);
      return {
        expression: `${attributeNameIdentifier} ${condition.type} ${attributeValueIdentifier}`,
      };
    } else {
      throw new InvalidDynamoDbConditionRequestError(`Unsupported condition operator ${condition.type}`);
    }
  }
}
