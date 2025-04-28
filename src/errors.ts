export class InvalidDynamoDbUpdateRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidDynamoDbUpdateRequestError';
  }
}

export class AttributeSessionFinalizedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AttributeSessionFinalizedError';
  }
}
