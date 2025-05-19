import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributeSessionFinalizedError } from './errors';

/**
 * Manage attribute names.
 */
export class AttributeNameSession {
  // map between segment to attribute name. e.g. position => #1, AAPL => #2, size => #3.
  private readonly attributeNameToIdentifier: Map<string, string>;

  private attributeNameCount: number;
  private isFinalized: boolean;

  constructor() {
    this.attributeNameToIdentifier = new Map();
    this.attributeNameCount = 0;
    this.isFinalized = false;
  }

  provideAttributeNameIdentifier(name: string): string {
    if (this.isFinalized) {
      throw new AttributeSessionFinalizedError('attribute names and values have been finalized.');
    }
    let identifier = this.attributeNameToIdentifier.get(name);
    if (typeof identifier !== 'string') {
      identifier = `#a${this.attributeNameCount}`;
      this.attributeNameCount += 1;
      this.attributeNameToIdentifier.set(name, identifier);
    }
    return identifier;
  }

  get expressionAttributeNames(): Record<string, string> {
    this.isFinalized = true;
    const identifierToAttributeName: Record<string, string> = {};

    this.attributeNameToIdentifier.forEach((identifier, name) => {
      identifierToAttributeName[identifier] = name;
    });

    return identifierToAttributeName;
  }
}

/**
 * Manage attribute values.
 */
export class AttributeValueSession {
  private readonly _expressionAttributeValues: Record<string, NativeAttributeValue>;

  private attributeValueCount: number;
  private isFinalized: boolean;

  constructor() {
    this._expressionAttributeValues = {};
    this.attributeValueCount = 0;
    this.isFinalized = false;
  }

  provideAttributeValueIdentifier(value: NativeAttributeValue): string {
    if (this.isFinalized) {
      throw new AttributeSessionFinalizedError('attribute names and values have been finalized.');
    }
    const identifier = `:v${this.attributeValueCount}`;
    this.attributeValueCount += 1;
    this._expressionAttributeValues[identifier] = value;
    return identifier;
  }

  get expressionAttributeValues(): Record<string, NativeAttributeValue> {
    this.isFinalized = true;
    return {
      ...this._expressionAttributeValues,
    };
  }
}
