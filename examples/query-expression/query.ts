import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { fromSSO } from '@aws-sdk/credential-providers';
import { buildDdbClient } from '../ddb-client-factory';
import { config } from 'dotenv';
import { getenv } from '../utils';
import { QueryExpressionBuilder } from '../../src';

config();

class Columns {
  static readonly PARTITION_KEY_COL = 'partitionKey';
  static readonly SORT_KEY_COL = 'sortKey';
}

export async function demo(docClient: DynamoDBDocument, tableName: string) {
  const partitionKey = 'ddb-utils';
  const sortKeyPrefix = 'v_';
  // for (let i = 0; i < 1_000; i++) {
  //   await docClient.put({
  //     TableName: tableName,
  //     Item: {
  //       [Columns.PARTITION_KEY_COL]: partitionKey,
  //       [Columns.SORT_KEY_COL]: sortKeyPrefix + i.toString().padStart(4, '0'),
  //       ['mydata']: Math.random()
  //     },
  //   });
  // }

  const queryExpressionBuilder = new QueryExpressionBuilder().key(Columns.PARTITION_KEY_COL, 'ddb-utils').between(Columns.SORT_KEY_COL, 'v_0146', 'v_0153').project('sortKey').project('mydata');

  const fitlering = queryExpressionBuilder.filterExpressionBuilder;
  const filter = fitlering.not(fitlering.and(fitlering.greaterThan('mydata', 0.2), fitlering.lessThan('mydata', 0.8)));

  const queryExpression = queryExpressionBuilder.build();
  const resp = await docClient.query({
    TableName: tableName,
    ReturnConsumedCapacity: 'TOTAL',
    ConsistentRead: true,
    KeyConditionExpression: queryExpression.keyConditionExpression,
    FilterExpression: filter.expression,
    ProjectionExpression: queryExpression.projectionExpression,
    ExpressionAttributeValues: queryExpression.expressionAttributeValues,
    ExpressionAttributeNames: queryExpression.expressionAttributeNames,
  });

  console.log(
    {
      TableName: tableName,
      ReturnConsumedCapacity: 'TOTAL',
      ConsistentRead: true,
      KeyConditionExpression: queryExpression.keyConditionExpression,
      FilterExpression: filter.expression,
      ProjectionExpression: queryExpression.projectionExpression,
      ExpressionAttributeValues: queryExpression.expressionAttributeValues,
      ExpressionAttributeNames: queryExpression.expressionAttributeNames,
    },
    null,
    2,
  );
  console.log(JSON.stringify(resp, null, 2));
}

(async () => {
  const docClient = buildDdbClient({
    region: getenv('AWS_REGION'),
    credentials: fromSSO({ profile: getenv('PROFILE_NAME') }),
  });

  await demo(docClient, getenv('TABLE_NAME'));
})();
