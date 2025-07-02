import { DynamoDBDocument, QueryCommandOutput, QueryCommandInput, BatchWriteCommandInput, BatchWriteCommandOutput } from '@aws-sdk/lib-dynamodb';
import { batchDelete } from '../src/batch-delete';
import { QueryExpression } from '../src/query-expression-builder';

// Mock the AWS SDK
jest.mock('@aws-sdk/lib-dynamodb');

// Helper function to create mock QueryCommandOutput
const createMockQueryOutput = (items: any[] | undefined, lastEvaluatedKey?: any): QueryCommandOutput => ({
  Items: items,
  LastEvaluatedKey: lastEvaluatedKey,
  $metadata: {},
});

const createMockBatchWriteCommandOutput = (unprocessedItems?: any): BatchWriteCommandOutput => ({
  UnprocessedItems: unprocessedItems,
  $metadata: {},
});

//     put(args: PutCommandInput, options?: __HttpHandlerOptions): Promise<PutCommandOutput>;
describe('batch-delete', () => {
  let mockDocClient: jest.Mocked<DynamoDBDocument>;
  let mockQuery: jest.MockedFunction<(input: QueryCommandInput) => Promise<QueryCommandOutput>>;
  let mockBatchWrite: jest.MockedFunction<(input: BatchWriteCommandInput) => Promise<BatchWriteCommandOutput>>;

  beforeEach(() => {
    // Clear all mocks before each test
    jest.clearAllMocks();

    // Create mock functions
    mockQuery = jest.fn();
    mockBatchWrite = jest.fn();

    // Create mock DynamoDBDocument
    mockDocClient = {
      query: mockQuery,
      batchWrite: mockBatchWrite,
    } as any;
  });

  const mockQueryExpression: QueryExpression = {
    keyConditionExpression: '#a0 = :v0',
    expressionAttributeNames: {
      '#a0': 'userId',
    },
    expressionAttributeValues: {
      ':v0': 'user123',
    },
  };

  const mockToKey = (item: Record<string, any>) => ({
    userId: item.userId,
    timestamp: item.timestamp,
  });

  describe('successful scenarios', () => {
    test('should delete items when query returns single page of results', async () => {
      const mockItems = [
        { userId: 'user123', timestamp: '2024-01-01', data: 'test1' },
        { userId: 'user123', timestamp: '2024-01-02', data: 'test2' },
      ];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 2,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockQuery).toHaveBeenCalledWith({
        TableName: 'test-table',
        IndexName: undefined,
        ExclusiveStartKey: undefined,
        ScanIndexForward: true,
        KeyConditionExpression: mockQueryExpression.keyConditionExpression,
        ExpressionAttributeValues: mockQueryExpression.expressionAttributeValues,
        ExpressionAttributeNames: mockQueryExpression.expressionAttributeNames,
        ProjectionExpression: mockQueryExpression.projectionExpression,
      });

      expect(mockBatchWrite).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).toHaveBeenCalledWith({
        RequestItems: {
          'test-table': [
            {
              DeleteRequest: {
                Key: { userId: 'user123', timestamp: '2024-01-01' },
              },
            },
            {
              DeleteRequest: {
                Key: { userId: 'user123', timestamp: '2024-01-02' },
              },
            },
          ],
        },
      });
    });

    test('should handle multiple pages of query results', async () => {
      const mockItems1 = [
        { userId: 'user123', timestamp: '2024-01-01', data: 'test1' },
        { userId: 'user123', timestamp: '2024-01-02', data: 'test2' },
      ];

      const mockItems2 = [{ userId: 'user123', timestamp: '2024-01-03', data: 'test3' }];

      const mockQueryOutput1 = createMockQueryOutput(mockItems1, { userId: 'user123', timestamp: '2024-01-02' });
      const mockQueryOutput2 = createMockQueryOutput(mockItems2);

      mockQuery.mockResolvedValueOnce(mockQueryOutput1).mockResolvedValueOnce(mockQueryOutput2);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', 'test-index', mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 3,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(2);
      expect(mockBatchWrite).toHaveBeenCalledTimes(2);

      // First batch write call for first page
      expect(mockBatchWrite).toHaveBeenNthCalledWith(1, {
        RequestItems: {
          'test-table': [
            {
              DeleteRequest: {
                Key: { userId: 'user123', timestamp: '2024-01-01' },
              },
            },
            {
              DeleteRequest: {
                Key: { userId: 'user123', timestamp: '2024-01-02' },
              },
            },
          ],
        },
      });

      // Second batch write call for second page
      expect(mockBatchWrite).toHaveBeenNthCalledWith(2, {
        RequestItems: {
          'test-table': [
            {
              DeleteRequest: {
                Key: { userId: 'user123', timestamp: '2024-01-03' },
              },
            },
          ],
        },
      });
    });

    test('should handle more than 25 items by batching into multiple requests', async () => {
      // Create 30 items (more than the 25 limit)
      const mockItems = Array.from({ length: 30 }, (_, i) => ({
        userId: 'user123',
        timestamp: `2024-01-${String(i + 1).padStart(2, '0')}`,
        data: `test${i + 1}`,
      }));

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 30,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).toHaveBeenCalledTimes(2);

      // First batch should have 25 items
      expect(mockBatchWrite).toHaveBeenNthCalledWith(1, {
        RequestItems: {
          'test-table': expect.arrayContaining([
            expect.objectContaining({
              DeleteRequest: {
                Key: expect.objectContaining({
                  userId: 'user123',
                }),
              },
            }),
          ]),
        },
      });

      // Second batch should have 5 items
      expect(mockBatchWrite).toHaveBeenNthCalledWith(2, {
        RequestItems: {
          'test-table': expect.arrayContaining([
            expect.objectContaining({
              DeleteRequest: {
                Key: expect.objectContaining({
                  userId: 'user123',
                }),
              },
            }),
          ]),
        },
      });

      // Verify the total number of delete requests
      const allCalls = mockBatchWrite.mock.calls;
      const totalDeleteRequests = allCalls.reduce((total: number, call: any[]) => {
        const requestItems = call[0].RequestItems['test-table'];
        return total + requestItems.length;
      }, 0);
      expect(totalDeleteRequests).toBe(30);
    });

    test('should return 0 when no items are found', async () => {
      const mockQueryOutput = createMockQueryOutput([]);

      mockQuery.mockResolvedValue(mockQueryOutput);

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 0,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).not.toHaveBeenCalled();
    });

    test('should handle undefined Items in query response', async () => {
      const mockQueryOutput = createMockQueryOutput(undefined);

      mockQuery.mockResolvedValue(mockQueryOutput);

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 0,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).not.toHaveBeenCalled();
    });

    test('should work with custom toKey function', async () => {
      const mockItems = [{ userId: 'user123', timestamp: '2024-01-01', data: 'test1', extra: 'ignored' }];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const customToKey = (item: Record<string, any>) => ({
        pk: item.userId,
        sk: item.timestamp,
      });

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, customToKey);

      expect(result).toEqual({
        deleteCount: 1,
        unprocessedKeys: [],
      });
      expect(mockBatchWrite).toHaveBeenCalledWith({
        RequestItems: {
          'test-table': [
            {
              DeleteRequest: {
                Key: { pk: 'user123', sk: '2024-01-01' },
              },
            },
          ],
        },
      });
    });

    test('should handle unprocessed items from batch write', async () => {
      const mockItems = [
        { userId: 'user123', timestamp: '2024-01-01', data: 'test1' },
        { userId: 'user123', timestamp: '2024-01-02', data: 'test2' },
      ];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);

      // Mock batch write returning unprocessed items
      const unprocessedItems = {
        'test-table': [
          {
            DeleteRequest: {
              Key: { userId: 'user123', timestamp: '2024-01-01' },
            },
          },
        ],
      };
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput(unprocessedItems));

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 1,
        unprocessedKeys: [{ userId: 'user123', timestamp: '2024-01-01' }],
      });
    });

    test('should handle multiple pages with unprocessed items', async () => {
      const mockItems1 = [
        { userId: 'user123', timestamp: '2024-01-01', data: 'test1' },
        { userId: 'user123', timestamp: '2024-01-02', data: 'test2' },
      ];

      const mockItems2 = [{ userId: 'user123', timestamp: '2024-01-03', data: 'test3' }];

      const mockQueryOutput1 = createMockQueryOutput(mockItems1, { userId: 'user123', timestamp: '2024-01-02' });
      const mockQueryOutput2 = createMockQueryOutput(mockItems2);

      mockQuery.mockResolvedValueOnce(mockQueryOutput1).mockResolvedValueOnce(mockQueryOutput2);

      // First batch has unprocessed items, second batch succeeds
      const unprocessedItems1 = {
        'test-table': [
          {
            DeleteRequest: {
              Key: { userId: 'user123', timestamp: '2024-01-01' },
            },
          },
        ],
      };
      mockBatchWrite.mockResolvedValueOnce(createMockBatchWriteCommandOutput(unprocessedItems1)).mockResolvedValueOnce(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', 'test-index', mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 2,
        unprocessedKeys: [{ userId: 'user123', timestamp: '2024-01-01' }],
      });
    });
  });

  describe('error scenarios', () => {
    test('should propagate query errors', async () => {
      const queryError = new Error('Query failed');
      mockQuery.mockRejectedValue(queryError);

      await expect(batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey)).rejects.toThrow('Query failed');

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).not.toHaveBeenCalled();
    });

    test('should propagate batchWrite errors', async () => {
      const mockItems = [{ userId: 'user123', timestamp: '2024-01-01', data: 'test1' }];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);

      const batchWriteError = new Error('Batch write failed');
      mockBatchWrite.mockRejectedValue(batchWriteError);

      await expect(batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey)).rejects.toThrow('Batch write failed');

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).toHaveBeenCalledTimes(1);
    });

    test('should handle batchWrite errors in multi-batch scenario', async () => {
      // Create 30 items to trigger multiple batches
      const mockItems = Array.from({ length: 30 }, (_, i) => ({
        userId: 'user123',
        timestamp: `2024-01-${String(i + 1).padStart(2, '0')}`,
        data: `test${i + 1}`,
      }));

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);

      const batchWriteError = new Error('Batch write failed');
      mockBatchWrite.mockRejectedValue(batchWriteError);

      await expect(batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey)).rejects.toThrow('Batch write failed');

      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).toHaveBeenCalledTimes(1);
    });
  });

  describe('edge cases', () => {
    test('should handle empty array of items', async () => {
      const mockQueryOutput = createMockQueryOutput([]);

      mockQuery.mockResolvedValue(mockQueryOutput);

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 0,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).not.toHaveBeenCalled();
    });

    test('should handle exactly 25 items (batch boundary)', async () => {
      const mockItems = Array.from({ length: 25 }, (_, i) => ({
        userId: 'user123',
        timestamp: `2024-01-${String(i + 1).padStart(2, '0')}`,
        data: `test${i + 1}`,
      }));

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 25,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).toHaveBeenCalledTimes(1);
    });

    test('should handle exactly 26 items (crosses batch boundary)', async () => {
      const mockItems = Array.from({ length: 26 }, (_, i) => ({
        userId: 'user123',
        timestamp: `2024-01-${String(i + 1).padStart(2, '0')}`,
        data: `test${i + 1}`,
      }));

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 26,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockBatchWrite).toHaveBeenCalledTimes(2);
    });

    test('should handle query with projection expression', async () => {
      const mockQueryExpressionWithProjection: QueryExpression = {
        keyConditionExpression: '#a0 = :v0',
        projectionExpression: '#a1, #a2',
        expressionAttributeNames: {
          '#a0': 'userId',
          '#a1': 'timestamp',
          '#a2': 'data',
        },
        expressionAttributeValues: {
          ':v0': 'user123',
        },
      };

      const mockItems = [{ userId: 'user123', timestamp: '2024-01-01', data: 'test1' }];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput());

      const result = await batchDelete(mockDocClient, 'test-table', 'test-index', mockQueryExpressionWithProjection, mockToKey);

      expect(result).toEqual({
        deleteCount: 1,
        unprocessedKeys: [],
      });
      expect(mockQuery).toHaveBeenCalledWith({
        TableName: 'test-table',
        IndexName: 'test-index',
        ExclusiveStartKey: undefined,
        ScanIndexForward: true,
        KeyConditionExpression: mockQueryExpressionWithProjection.keyConditionExpression,
        ExpressionAttributeValues: mockQueryExpressionWithProjection.expressionAttributeValues,
        ExpressionAttributeNames: mockQueryExpressionWithProjection.expressionAttributeNames,
        ProjectionExpression: mockQueryExpressionWithProjection.projectionExpression,
      });
    });

    test('should handle undefined UnprocessedItems in batch write response', async () => {
      const mockItems = [{ userId: 'user123', timestamp: '2024-01-01', data: 'test1' }];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);

      // Mock batch write returning undefined UnprocessedItems
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput(undefined));

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 1,
        unprocessedKeys: [],
      });
    });

    test('should handle empty UnprocessedItems in batch write response', async () => {
      const mockItems = [{ userId: 'user123', timestamp: '2024-01-01', data: 'test1' }];

      const mockQueryOutput = createMockQueryOutput(mockItems);

      mockQuery.mockResolvedValue(mockQueryOutput);

      // Mock batch write returning empty UnprocessedItems
      mockBatchWrite.mockResolvedValue(createMockBatchWriteCommandOutput({}));

      const result = await batchDelete(mockDocClient, 'test-table', undefined, mockQueryExpression, mockToKey);

      expect(result).toEqual({
        deleteCount: 1,
        unprocessedKeys: [],
      });
    });
  });
});
