import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { fromSSO } from '@aws-sdk/credential-providers';
import { buildDdbClient } from '../ddb-client-factory';
import { config } from 'dotenv';
import { getenv } from '../utils';
import { batchDelete, QueryExpressionBuilder } from '../../src';

config();

class Columns {
  static readonly PARTITION_KEY_COL = 'partitionKey';
  static readonly SORT_KEY_COL = 'sortKey';
}

export async function demo(docClient: DynamoDBDocument, tableName: string) {
  const partitionKey = 'ddb-utils';
  const sortKeyPrefix = 'v_';
  for (let i = 0; i < 100; i++) {
    await docClient.put({
      TableName: tableName,
      Item: {
        [Columns.PARTITION_KEY_COL]: partitionKey,
        [Columns.SORT_KEY_COL]: sortKeyPrefix + i.toString().padStart(4, '0'),
        ['mydata']: 'a'.repeat(399 * 1024),
      },
    });
  }

  const queryExpressionBuilder = new QueryExpressionBuilder().key(Columns.PARTITION_KEY_COL, 'ddb-utils').between(Columns.SORT_KEY_COL, 'v_0000', 'v_1000');
  const queryExpression = queryExpressionBuilder.build();

  const output = await batchDelete(docClient, tableName, undefined, queryExpression, (item) => {
    return {
      [Columns.PARTITION_KEY_COL]: item[Columns.PARTITION_KEY_COL],
      [Columns.SORT_KEY_COL]: item[Columns.SORT_KEY_COL],
    };
  });
  console.log(output);
}

(async () => {
  const docClient = buildDdbClient({
    region: getenv('AWS_REGION'),
    credentials: fromSSO({ profile: getenv('PROFILE_NAME') }),
  });

  await demo(docClient, getenv('TABLE_NAME'));
})();
