import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { fromSSO } from '@aws-sdk/credential-providers';
import { buildDdbClient } from '../ddb-client-factory';
import { config } from 'dotenv';
import { getenv } from '../utils';
import { UpdateExpressionBuilder } from '../../src';

config();

class Columns {
  static readonly PARTITION_KEY_COL = 'partitionKey';
  static readonly SORT_KEY_COL = 'sortKey';
  static readonly LIST_COL = 'list';
}

export async function demo(docClient: DynamoDBDocument, tableName: string) {
  const partitionKey = 'ddb-utils';
  const sortKey = 'v4';
  await docClient.put({
    TableName: tableName,
    Item: {
      [Columns.PARTITION_KEY_COL]: partitionKey,
      [Columns.SORT_KEY_COL]: sortKey,
      [Columns.LIST_COL]: ['ddb', 'aws'],
    },
  });

  const updateExpressionBuilder = new UpdateExpressionBuilder();
  updateExpressionBuilder.append(Columns.LIST_COL, {
    public: true,
    'release-date': '2024-10-10',
  });
  const updateExpression = updateExpressionBuilder.build();

  console.log(JSON.stringify(updateExpression, null, 2));

  await docClient.update({
    TableName: tableName,
    Key: {
      [Columns.PARTITION_KEY_COL]: partitionKey,
      [Columns.SORT_KEY_COL]: sortKey,
    },
    /**
     * Can't use AttributeUpdates because it uses non-parameter update expression, which is considered legacy and doesn't work
     * with ConditionExpression
     */
    // AttributeUpdates: this.convertUpdateUserProfileRequestToAttributeUpdates(request),
    UpdateExpression: updateExpression.updateExpression,
    ExpressionAttributeValues: updateExpression.expressionAttributeValues,
    ExpressionAttributeNames: updateExpression.expressionAttributeNames,
  });
}

(async () => {
  const docClient = buildDdbClient({
    region: getenv('AWS_REGION'),
    credentials: fromSSO({ profile: getenv('SSO_PROFILE_NAME') }),
  });

  await demo(docClient, getenv('TABLE_NAME'));
})();
