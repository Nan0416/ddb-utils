import { DynamoDBDocument, QueryCommandInput } from '@aws-sdk/lib-dynamodb';
import { fromSSO } from '@aws-sdk/credential-providers';
import { buildDdbClient } from '../ddb-client-factory';
import { config } from 'dotenv';
import { getenv } from '../utils';
import { QueryExpressionBuilder } from '../../src';

config();

class Columns {
  static readonly PARTITION_KEY_COL = 'partitionKey';
  static readonly SORT_KEY_COL = 'sortKey';
  static readonly LOCAL_SORT_KEY_COL = 'localSortKey';
  static readonly GSI_PARTITION_KEY_COL = 'ownerId';
  static readonly DATA_KEY = 'mydata';
  static readonly DATA_KEY_2 = 'mydata.p1';
}

export async function demo(docClient: DynamoDBDocument, tableName: string) {
  const partitionKey = 'ddb-utils';
  const sortKeyPrefix = 'v_';
  //   for (let i = 0; i < 100; i++) {
  //     await docClient.put({
  //       TableName: tableName,
  //       Item: {
  //         [Columns.PARTITION_KEY_COL]: partitionKey,
  //         [Columns.SORT_KEY_COL]: sortKeyPrefix + i.toString().padStart(4, '0'),
  //         [Columns.LOCAL_SORT_KEY_COL]: i < 20 ? undefined : (i % 10).toString(),
  //         [Columns.DATA_KEY]: Math.random(),
  //         [Columns.DATA_KEY_2]: Math.random(),
  //       },
  //     });
  //   }

  //   const queryExpressionBuilder = new QueryExpressionBuilder().key(Columns.GSI_PARTITION_KEY_COL, '99').equal(Columns.PARTITION_KEY_COL, 'ddb-utilsx');
  //   const queryExpression = queryExpressionBuilder.build();
  //   const queryInput: QueryCommandInput = {
  //     TableName: tableName,
  //     IndexName: 'ownerId-partitionKey-index',
  //     ReturnConsumedCapacity: 'TOTAL',
  //     ConsistentRead: false, // GSI doesn't support consistent read.
  //     KeyConditionExpression: queryExpression.keyConditionExpression,
  //     ExpressionAttributeValues: queryExpression.expressionAttributeValues,
  //     ExpressionAttributeNames: queryExpression.expressionAttributeNames,
  //   };
  //   const resp = await docClient.query(queryInput);

  const queryExpressionBuilder = new QueryExpressionBuilder().key(Columns.PARTITION_KEY_COL, 'ddb-utils').equal(Columns.LOCAL_SORT_KEY_COL, '3');

  const filtering = queryExpressionBuilder.filterExpressionBuilder;
  const filter = filtering.equal(Columns.SORT_KEY_COL, 'v_0093');

  const queryExpression = queryExpressionBuilder.build();
  const queryInput: QueryCommandInput = {
    TableName: tableName,
    IndexName: 'localSortKey-index',
    ReturnConsumedCapacity: 'TOTAL',
    ConsistentRead: true, // LSI support consistent read.
    KeyConditionExpression: queryExpression.keyConditionExpression,
    ExpressionAttributeValues: queryExpression.expressionAttributeValues,
    ExpressionAttributeNames: queryExpression.expressionAttributeNames,
    FilterExpression: filter.expression,
  };
  const resp = await docClient.query(queryInput);

  console.log(queryInput, null, 2);
  console.log(JSON.stringify(resp, null, 2));
}

(async () => {
  const docClient = buildDdbClient({
    region: getenv('AWS_REGION'),
    credentials: fromSSO({ profile: getenv('PROFILE_NAME') }),
  });

  await demo(docClient, getenv('TABLE_NAME'));
})();
