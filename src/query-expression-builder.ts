import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributeNameSession, AttributeValueSession } from './attribute-session';
import { InvalidDynamoDbQueryRequestError, QueryConditionConflictError } from './errors';
import { ProjectionExpressionBuilder } from './projection-expression-builder';
import { ConditionExpressionBuilder } from './condition-expression-builder';

export interface QueryExpression {
  readonly keyConditionExpression: string;
  readonly projectionExpression?: string;
  readonly expressionAttributeValues: Record<string, NativeAttributeValue>;
  readonly expressionAttributeNames: Record<string, string>;
}

interface _Condition {
  readonly type: '=' | '<' | '<=' | '>' | '>=' | 'between' | 'begins_with';
  readonly sortKeyColumnName: string;
}

interface TwoOperandsCondition extends _Condition {
  readonly type: '=' | '<' | '<=' | '>' | '>=' | 'begins_with';
  readonly valueOperand: NativeAttributeValue;
}

interface BetweenCondition extends _Condition {
  readonly type: 'between';
  readonly greaterThanOrEqualTo: NativeAttributeValue;
  readonly lessThanOrEqualTo: NativeAttributeValue;
}

type Condition = TwoOperandsCondition | BetweenCondition;

/**
 * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html
 */
export class QueryExpressionBuilder {
  private readonly attributeNameSession: AttributeNameSession;
  private readonly attributeValueSession: AttributeValueSession;
  private readonly projectionExpressionBuilder: ProjectionExpressionBuilder;
  readonly filterExpressionBuilder: ConditionExpressionBuilder;
  private partitionKeyCondition?: string;
  private sortKeyCondition?: string;

  constructor() {
    this.attributeNameSession = new AttributeNameSession();
    this.attributeValueSession = new AttributeValueSession();
    this.projectionExpressionBuilder = new ProjectionExpressionBuilder(this.attributeNameSession);
    this.filterExpressionBuilder = new ConditionExpressionBuilder(this.attributeNameSession, this.attributeValueSession);
  }

  key(partitionKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    if (typeof this.partitionKeyCondition === 'string') {
      throw new QueryConditionConflictError('Query partition key is already defined.');
    }
    const attributeNameIdentifier = this.attributeNameSession.provideAttributeNameIdentifier(partitionKeyColumnName);
    const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(value);
    this.partitionKeyCondition = `${attributeNameIdentifier} = ${attributeValueIdentifier}`;
    return this;
  }

  lessThan(sortKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: '<',
      valueOperand: value,
    });
  }

  lessThanOrEqualTo(sortKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: '<=',
      valueOperand: value,
    });
  }

  equal(sortKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: '=',
      valueOperand: value,
    });
  }

  greaterThan(sortKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: '>',
      valueOperand: value,
    });
  }

  greaterThanOrEqualTo(sortKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: '>=',
      valueOperand: value,
    });
  }

  beginsWith(sortKeyColumnName: string, value: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: 'begins_with',
      valueOperand: value,
    });
  }

  project(path: string | ReadonlyArray<string>): QueryExpressionBuilder {
    this.projectionExpressionBuilder.get(path);
    return this;
  }
  /**
   *
   * @param left inclusive
   * @param right inclusive
   * @returns
   */
  between(sortKeyColumnName: string, left: NativeAttributeValue, right: NativeAttributeValue): QueryExpressionBuilder {
    return this.withSortKeyCondition({
      sortKeyColumnName: sortKeyColumnName,
      type: 'between',
      greaterThanOrEqualTo: left,
      lessThanOrEqualTo: right,
    });
  }
  /**
   * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.NestedAttributes
   * @param col
   * @param value null to delete, undefined to noop,
   * @returns
   */
  private withSortKeyCondition(op: Condition): QueryExpressionBuilder {
    if (typeof this.sortKeyCondition === 'string') {
      throw new QueryConditionConflictError('Query sort key is already defined.');
    }

    if (op.type === 'between') {
      // two operands
      const attributeNameIdentifier = this.attributeNameSession.provideAttributeNameIdentifier(op.sortKeyColumnName);
      const greaterThanOrEqualToAttributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.greaterThanOrEqualTo);
      const lessThanOrEqualToAttributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.lessThanOrEqualTo);
      this.sortKeyCondition = `(${attributeNameIdentifier} BETWEEN ${greaterThanOrEqualToAttributeValueIdentifier} AND ${lessThanOrEqualToAttributeValueIdentifier})`;
    } else if (op.type === 'begins_with') {
      // two operands
      const attributeNameIdentifier = this.attributeNameSession.provideAttributeNameIdentifier(op.sortKeyColumnName);
      const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.valueOperand);
      this.sortKeyCondition = `begins_with(${attributeNameIdentifier}, ${attributeValueIdentifier})`;
    } else {
      // two operands
      const attributeNameIdentifier = this.attributeNameSession.provideAttributeNameIdentifier(op.sortKeyColumnName);
      const attributeValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.valueOperand);
      this.sortKeyCondition = `${attributeNameIdentifier} ${op.type} ${attributeValueIdentifier}`;
    }
    return this;
  }

  build(): QueryExpression {
    if (this.partitionKeyCondition === undefined) {
      throw new InvalidDynamoDbQueryRequestError('Failed to build query expression because of missing partition key condition.');
    }

    let keyConditionExpression = this.partitionKeyCondition;
    if (typeof this.sortKeyCondition === 'string') {
      keyConditionExpression = `${keyConditionExpression} AND ${this.sortKeyCondition}`;
    }

    const projectionExpression = this.projectionExpressionBuilder.hasProjection() ? this.projectionExpressionBuilder.build().projectionExpression : undefined;
    return {
      keyConditionExpression: keyConditionExpression,
      expressionAttributeValues: this.attributeValueSession.expressionAttributeValues,
      expressionAttributeNames: this.attributeNameSession.expressionAttributeNames,
      projectionExpression: projectionExpression,
    };
  }
}
