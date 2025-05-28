export class InvalidDynamoDbUpdateRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidDynamoDbUpdateRequestError';
  }
}

export class InvalidDynamoDbProjectionRequestError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class InvalidDynamoDbQueryRequestError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class AttributeSessionFinalizedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AttributeSessionFinalizedError';
  }
}

export class QueryConditionConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'QueryConditionConflictError';
  }
}
