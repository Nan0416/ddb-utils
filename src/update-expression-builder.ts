import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributeSession } from './attribute-session';
import { ConditionExpressionBuilder } from './condition-expression-builder';
import { InvalidDynamoDbUpdateRequestError } from './errors';

export interface UpdateExpression {
  readonly updateExpression: string;
  readonly expressionAttributeValues: Record<string, NativeAttributeValue>;
  readonly expressionAttributeNames?: Record<string, string>;
}

export class UpdateExpressionBuilder {
  private readonly setStatements: string[];
  private readonly removeStatements: string[];
  private readonly attributeSession: AttributeSession;
  readonly conditionExpressionBuilder: ConditionExpressionBuilder;
  private visitedPaths: Set<string>;

  constructor() {
    this.setStatements = [];
    this.removeStatements = [];
    this.attributeSession = new AttributeSession();
    this.visitedPaths = new Set();
    this.conditionExpressionBuilder = new ConditionExpressionBuilder(this.attributeSession);
  }

  set(path: string | ReadonlyArray<string>, value: any): UpdateExpressionBuilder {
    return this.with(path, value);
  }

  delete(path: string | ReadonlyArray<string>): UpdateExpressionBuilder {
    return this.with(path, null);
  }

  /**
   * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.NestedAttributes
   * @param col
   * @param value null to delete, undefined to noop,
   * @returns
   */
  private with(path: string | ReadonlyArray<string>, value: null | undefined | any): UpdateExpressionBuilder {
    if (typeof path === 'string') {
      path = [path];
    }

    const pathIdentifier = path.join('.');

    if (this.visitedPaths.has(pathIdentifier)) {
      throw new InvalidDynamoDbUpdateRequestError(`Path ${pathIdentifier} is already in the update list.`);
    }

    if (value === undefined) {
      return this;
    }

    this.visitedPaths.add(pathIdentifier);
    const attributeNameIdentifiers: string[] = [];

    path.forEach((segment) => {
      const attributeNameIdentifier = this.attributeSession.provideAttributeNameIdentifier(segment);
      attributeNameIdentifiers.push(attributeNameIdentifier);
    });

    if (value === null) {
      this.removeStatements.push(attributeNameIdentifiers.join('.'));
    } else {
      const valueIdentifier = this.attributeSession.provideAttributeValueIdentifier(value);
      // if this is a nested path, ensure the top level exist before setting the value. Otherwise, it will throw
      // ValidationException: The document path provided in the update expression is invalid for update.
      this.setStatements.push(`${attributeNameIdentifiers.join('.')} = ${valueIdentifier}`);
    }
    return this;
  }

  hasUpdate() {
    return this.removeStatements.length > 0 || this.setStatements.length > 0;
  }

  build(): UpdateExpression {
    if (!this.hasUpdate()) {
      throw new InvalidDynamoDbUpdateRequestError("Update request can't be empty.");
    }

    const statements: string[] = [];
    if (this.setStatements.length > 0) {
      statements.push(`SET ${this.setStatements.join(', ')}`);
    }
    if (this.removeStatements.length > 0) {
      statements.push(`REMOVE ${this.removeStatements.join(', ')}`);
    }

    return {
      updateExpression: statements.join(' '),
      expressionAttributeValues: this.attributeSession.expressionAttributeValues,
      expressionAttributeNames: this.attributeSession.expressionAttributeNames,
    };
  }
}
