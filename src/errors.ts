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

export class InvalidDynamoDbConditionRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidDynamoDbConditionRequestError';
  }
}

/** An item attribute missing or of the wrong type; `key` names the attribute. */
export class ItemParseError extends Error {
  readonly key: string;

  constructor(key: string, message: string) {
    super(message);
    this.name = 'ItemParseError';
    this.key = key;
  }
}
