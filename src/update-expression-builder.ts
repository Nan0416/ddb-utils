import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributeNameSession, AttributeValueSession } from './attribute-session';
import { ConditionExpressionBuilder } from './condition-expression-builder';
import { InvalidDynamoDbUpdateRequestError } from './errors';

export interface UpdateExpression {
  readonly updateExpression: string;
  readonly expressionAttributeValues: Record<string, NativeAttributeValue>;
  readonly expressionAttributeNames: Record<string, string>;
}

interface _Operation {
  readonly type: 'delete' | 'set' | 'list_append';
}

interface DeleteOperation extends _Operation {
  readonly type: 'delete';
}

interface SetOperation extends _Operation {
  readonly type: 'set';
  readonly value: NativeAttributeValue;
}

interface ListAppendOperation extends _Operation {
  readonly type: 'list_append';
  readonly value: NativeAttributeValue;
  readonly position: 'start' | 'end';
  /**
   * When set to true, if the list doesn't exist in the item, the operation will populate an empty list and append the element.
   * If the allowListInit is set to false, it will throw an error if the existing item doesn't have the attribute.
   */
  readonly allowListInit: boolean;
}

type Operation = DeleteOperation | SetOperation | ListAppendOperation;

export class UpdateExpressionBuilder {
  private readonly setStatements: string[];
  private readonly removeStatements: string[];
  private readonly attributeNameSession: AttributeNameSession;
  private readonly attributeValueSession: AttributeValueSession;
  readonly conditionExpressionBuilder: ConditionExpressionBuilder;
  private visitedPaths: Set<string>;

  constructor() {
    this.setStatements = [];
    this.removeStatements = [];
    this.attributeNameSession = new AttributeNameSession();
    this.attributeValueSession = new AttributeValueSession();
    this.visitedPaths = new Set();
    this.conditionExpressionBuilder = new ConditionExpressionBuilder(this.attributeNameSession, this.attributeValueSession);
  }

  set(path: string | ReadonlyArray<string>, value: NativeAttributeValue): UpdateExpressionBuilder {
    return this.with(path, { type: 'set', value: value });
  }

  /**
   *
   * @param path
   * @param values
   * @param position @default end
   * @param failIfMissing @default false
   * @returns
   */
  append(path: string | ReadonlyArray<string>, value: NativeAttributeValue, position?: 'start' | 'end', failIfMissing?: boolean) {
    return this.with(path, {
      type: 'list_append',
      value: value,
      position: position ?? 'end',
      allowListInit: !failIfMissing,
    });
  }

  delete(path: string | ReadonlyArray<string>): UpdateExpressionBuilder {
    return this.with(path, { type: 'delete' });
  }

  /**
   * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.NestedAttributes
   * @param col
   * @param value null to delete, undefined to noop,
   * @returns
   */
  private with(path: string | ReadonlyArray<string>, op: Operation): UpdateExpressionBuilder {
    if (typeof path === 'string') {
      path = [path];
    }

    const pathIdentifier = path.join('.');

    if (this.visitedPaths.has(pathIdentifier)) {
      throw new InvalidDynamoDbUpdateRequestError(`Path ${pathIdentifier} is already in the update list.`);
    }

    this.visitedPaths.add(pathIdentifier);
    const attributeNameIdentifiers: string[] = [];

    path.forEach((segment) => {
      const attributeNameIdentifier = this.attributeNameSession.provideAttributeNameIdentifier(segment);
      attributeNameIdentifiers.push(attributeNameIdentifier);
    });

    if (op.type === 'delete') {
      this.removeStatements.push(attributeNameIdentifiers.join('.'));
    } else if (op.type === 'set') {
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.value);
      // if this is a nested path, ensure the top level exist before setting the value. Otherwise, it will throw
      // ValidationException: The document path provided in the update expression is invalid for update.
      this.setStatements.push(`${attributeNameIdentifiers.join('.')} = ${valueIdentifier}`);
    } else if (op.type === 'list_append') {
      const value = Array.isArray(op.value) ? op.value : [op.value];
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(value);
      const attributePath = attributeNameIdentifiers.join('.');
      let attributePathOperand = attributePath;

      if (op.allowListInit) {
        const emptyListIdentitifer = this.attributeValueSession.provideAttributeValueIdentifier([]);
        attributePathOperand = `if_not_exists(${attributePath}, ${emptyListIdentitifer})`;
      }

      if (op.position === 'start') {
        this.setStatements.push(`${attributePath} = list_append(${valueIdentifier}, ${attributePathOperand})`);
      } else {
        this.setStatements.push(`${attributePath} = list_append(${attributePathOperand}, ${valueIdentifier})`);
      }
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
      expressionAttributeValues: this.attributeValueSession.expressionAttributeValues,
      expressionAttributeNames: this.attributeNameSession.expressionAttributeNames,
    };
  }
}
