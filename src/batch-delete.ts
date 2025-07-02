import { DynamoDBDocument, NativeAttributeValue, QueryCommandOutput } from '@aws-sdk/lib-dynamodb';
import lodash from 'lodash';
import { QueryExpression } from './query-expression-builder';

const ITEM_TO_DELETE_PER_BATCH = 25;

export interface BatchDeleteOutput {
  readonly deleteCount: number;
  readonly unprocessedKeys: Record<string, NativeAttributeValue>[];
}

export async function batchDelete(
  docClient: DynamoDBDocument,
  tableName: string,
  indexName: string | undefined,
  query: QueryExpression,
  toKey: (item: Record<string, NativeAttributeValue>) => Record<string, NativeAttributeValue>,
): Promise<BatchDeleteOutput> {
  let lastEvaluatedKey: Record<string, NativeAttributeValue> | undefined = undefined;
  let deletedItemsCount = 0;
  let unprocessedKeys: Record<string, NativeAttributeValue>[] = [];
  while (true) {
    /**
     * Query operates on the same partition, each query returns maximum 1MB data.
     * batchWrite will then operate the same partition as well, and the write operation also has a maximum 1MB per partition.
     */
    const queryOutput: QueryCommandOutput = await docClient.query({
      TableName: tableName,
      IndexName: indexName,
      ExclusiveStartKey: lastEvaluatedKey,
      ScanIndexForward: true,
      KeyConditionExpression: query.keyConditionExpression,
      ExpressionAttributeValues: query.expressionAttributeValues,
      ExpressionAttributeNames: query.expressionAttributeNames,
      ProjectionExpression: query.projectionExpression,
    });

    const itemsToDelete: Record<string, NativeAttributeValue>[] = [];
    queryOutput.Items?.forEach((item) => itemsToDelete.push(toKey(item)));

    const deleteOutput = await batchDeleteWithProtectedWriteThroughput(docClient, tableName, itemsToDelete);
    deletedItemsCount += deleteOutput.deleteCount;
    unprocessedKeys = unprocessedKeys.concat(deleteOutput.unprocessedKeys);

    if (queryOutput.LastEvaluatedKey === undefined) {
      break;
    } else {
      lastEvaluatedKey = queryOutput.LastEvaluatedKey;
    }
  }

  return {
    deleteCount: deletedItemsCount,
    unprocessedKeys: unprocessedKeys,
  };
}

async function batchDeleteWithProtectedWriteThroughput(docClient: DynamoDBDocument, tableName: string, itemsToDelete: Record<string, NativeAttributeValue>[]): Promise<BatchDeleteOutput> {
  if (itemsToDelete.length === 0) {
    return { deleteCount: 0, unprocessedKeys: [] };
  }

  const unprocessedKeys: Record<string, NativeAttributeValue>[] = [];
  // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
  // Maximum 25 items in a batch request, otherwise, it throws exception.
  const partitionedKeys = lodash.chunk(itemsToDelete, ITEM_TO_DELETE_PER_BATCH);
  for (let i = 0; i < partitionedKeys.length; i++) {
    const resp = await docClient.batchWrite({
      RequestItems: {
        [tableName]: partitionedKeys[i].map((item) => {
          return {
            DeleteRequest: {
              Key: item,
            },
          };
        }),
      },
    });

    if (resp.UnprocessedItems) {
      resp.UnprocessedItems[tableName]?.forEach((item) => {
        if (item.DeleteRequest?.Key) {
          unprocessedKeys.push(item.DeleteRequest.Key);
        }
      });
    }
  }

  return {
    deleteCount: itemsToDelete.length - unprocessedKeys.length,
    unprocessedKeys: unprocessedKeys,
  };
}
