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
 *
 * Throughtput https://aws.amazon.com/blogs/database/part-1-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/
 * A DynamoDB table data is stored into several physical partition based on the partition key (and sometime sort key). Each partition
 * support 3000 strong consistent read units per second and 1000 write unit per second. The 3000 and 1000 are hard limit. To increase throughput, DynamoDB can
 * divide one partition into two partitions based on the sort key. However, if the application has a hot partition key, the application developer has to
 * re-design the hot partition key.
 *
 * By default, a DynamoDB table come with 3 partitions. And each partition has a primary node and two replica in different AZs. The primary node is used to support
 * all write operation and strong consistence read. For eventual consistence read, it can read from the replica. And each partition node can handle 3000 RCU and
 * 1000 WCU. It means each partition can support 3000 SC RCU or 9000 eventual consistence (EC) RCU, and 1000 WCU. However, considering any of the node be turn down
 * due to deployment, the guaranteed EC RCU is 6000.
 *
 * What does capacity unit mean? https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
 *
 * 1. One read capacity unit means 4KB of data. One partition node supports 3000 RCU, it means the maximum I/O throughput is 12MB/second for read access.
 * 2. One write capacity unit means 1KB of data. One partition node supports 1000 WCU, it means the maximum I/O throughput is 1MB/second for write access.
 * 2. How does DynamoDB calculates the by consumed capacity? It depends on the operation and item size.
 *  * For strong consistence read, it equals ceil(item size/4KB). Example, reading an item whose size is around 900 bytes, then the RCU ceil (0.9/4KB) = 1.
 *  * BatchGetItem is same as read item individually, for each item, it round up to 4KB, and then sum.
 *  * Query is different from BatchGetItem, it sums up the total item size, and then divided by 4KB.
 *
 * Example 1, I have a DDB table and each item size is near the maximum limit 400KB. I want to use Query to get items on the same partition, how many
 * items will I get in one Query request? And how many concurrent Query can you have one the same partiton key?
 *
 * One query operation support up to 1MB of data, it means 1MB/400KB = 2.
 *
 * The maximum number of concurrent query also depends on if the request is strong consistence or eventual consistence. For eventual consistence, in most case, you have
 * one primary node and two replica can execute the query, and one node can support 3000 * 4KB = 12MB, you can a maximum read throughput of 36MB, meaning, a 36 concurrent
 * query on the same partition.
 *
 * Example 2, I have a DDB table and each item size is near the maximum limit 400KB. I want to use BatchWrite to delete items on the same partition, how many
 * items can I delete in one BatchWrite request?
 *
 * First, the maximum item in one BatchWrite operation is 25. Second, all items are in the same partition, the maximum write throughput is 1000 * 1KB = 1MB,
 * divided by 400 KB per item, the maximum items in one request is 2.
 *
 *  Min(25, 1000 WCU * 1(KB/WCU) / 400(KB/Item)) = 2.
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
