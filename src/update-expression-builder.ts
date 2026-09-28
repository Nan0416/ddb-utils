import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';
import { AttributePath, pathKey, renderPath } from './attribute-path';
import { AttributeNameSession, AttributeValueSession } from './attribute-session';
import { ConditionExpressionBuilder } from './condition-expression-builder';
import { InvalidDynamoDbUpdateRequestError } from './errors';

export interface UpdateExpression {
  readonly updateExpression: string;
  readonly expressionAttributeValues: Record<string, NativeAttributeValue>;
  readonly expressionAttributeNames: Record<string, string>;
}

interface _Operation {
  readonly type: 'delete' | 'set' | 'set_if_not_exists' | 'increment' | 'list_append' | 'add_to_set' | 'delete_from_set';
}

interface DeleteOperation extends _Operation {
  readonly type: 'delete';
}

interface SetOperation extends _Operation {
  readonly type: 'set';
  readonly value: NativeAttributeValue;
}

/** SET path = if_not_exists(path, value): writes the value only when the attribute is absent. */
interface SetIfNotExistsOperation extends _Operation {
  readonly type: 'set_if_not_exists';
  readonly value: NativeAttributeValue;
}

/** SET path = path + delta, or if_not_exists(path, initialValue) + delta when an initial value is given. */
interface IncrementOperation extends _Operation {
  readonly type: 'increment';
  readonly delta: number;
  readonly initialValue?: number;
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

/** DynamoDB ADD action — adds elements to a String Set, Number Set, or Binary Set. Also works for incrementing a number. */
interface AddToSetOperation extends _Operation {
  readonly type: 'add_to_set';
  readonly value: NativeAttributeValue;
}

/** DynamoDB DELETE action — removes elements from a String Set, Number Set, or Binary Set. */
interface DeleteFromSetOperation extends _Operation {
  readonly type: 'delete_from_set';
  readonly value: NativeAttributeValue;
}

type Operation = DeleteOperation | SetOperation | SetIfNotExistsOperation | IncrementOperation | ListAppendOperation | AddToSetOperation | DeleteFromSetOperation;

export interface SetFieldsOptions {
  /** REMOVE an attribute whose value is null instead of storing NULL. @default false */
  readonly removeNulls?: boolean;
}

export class UpdateExpressionBuilder {
  private readonly setStatements: string[];
  private readonly removeStatements: string[];
  private readonly addStatements: string[];
  private readonly deleteFromSetStatements: string[];
  private readonly attributeNameSession: AttributeNameSession;
  private readonly attributeValueSession: AttributeValueSession;
  readonly conditionExpressionBuilder: ConditionExpressionBuilder;
  private visitedPaths: Set<string>;

  constructor() {
    this.setStatements = [];
    this.removeStatements = [];
    this.addStatements = [];
    this.deleteFromSetStatements = [];
    this.attributeNameSession = new AttributeNameSession();
    this.attributeValueSession = new AttributeValueSession();
    this.visitedPaths = new Set();
    this.conditionExpressionBuilder = new ConditionExpressionBuilder(this.attributeNameSession, this.attributeValueSession);
  }

  set(path: AttributePath, value: NativeAttributeValue): UpdateExpressionBuilder {
    return this.with(path, { type: 'set', value: value });
  }

  /**
   * One `set` per top-level field. An undefined value is skipped, so a partial-update object
   * can be passed as it is; a null value is stored as NULL, or removed with `removeNulls`.
   */
  setFields(fields: Readonly<Record<string, NativeAttributeValue>>, options?: SetFieldsOptions): UpdateExpressionBuilder {
    for (const [field, value] of Object.entries(fields)) {
      if (value === undefined) {
        continue;
      }
      if (value === null && options?.removeNulls === true) {
        this.delete(field);
      } else {
        this.set(field, value);
      }
    }
    return this;
  }

  /** Writes the value only when the attribute is absent; an existing value is kept. */
  setIfNotExists(path: AttributePath, value: NativeAttributeValue): UpdateExpressionBuilder {
    return this.with(path, { type: 'set_if_not_exists', value: value });
  }

  /**
   * Adds `delta` (negative to subtract) to a number attribute. Without `initialValue` the
   * attribute must exist, or DynamoDB rejects the update; with it, a missing attribute counts
   * from `initialValue`.
   */
  increment(path: AttributePath, delta: number, initialValue?: number): UpdateExpressionBuilder {
    return this.with(path, { type: 'increment', delta: delta, initialValue: initialValue });
  }

  /**
   *
   * @param path
   * @param values
   * @param position @default end
   * @param failIfMissing @default false
   * @returns
   */
  append(path: AttributePath, value: NativeAttributeValue, position?: 'start' | 'end', failIfMissing?: boolean) {
    return this.with(path, {
      type: 'list_append',
      value: value,
      position: position ?? 'end',
      allowListInit: !failIfMissing,
    });
  }

  delete(path: AttributePath): UpdateExpressionBuilder {
    return this.with(path, { type: 'delete' });
  }

  /**
   * DynamoDB ADD action — adds elements to a Set (String Set, Number Set, or Binary Set),
   * or increments a number attribute. Uses the DynamoDB `ADD` update action.
   * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.ADD
   */
  addToSet(path: AttributePath, value: NativeAttributeValue): UpdateExpressionBuilder {
    return this.with(path, { type: 'add_to_set', value: value });
  }

  /**
   * DynamoDB DELETE action — removes elements from a Set (String Set, Number Set, or Binary Set).
   * Uses the DynamoDB `DELETE` update action.
   * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.DELETE
   */
  deleteFromSet(path: AttributePath, value: NativeAttributeValue): UpdateExpressionBuilder {
    return this.with(path, { type: 'delete_from_set', value: value });
  }

  /**
   * Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html#Expressions.ExpressionAttributeNames.NestedAttributes
   */
  private with(path: AttributePath, op: Operation): UpdateExpressionBuilder {
    const key = pathKey(path);
    if (this.visitedPaths.has(key)) {
      throw new InvalidDynamoDbUpdateRequestError(`Path ${key} is already in the update list.`);
    }
    this.visitedPaths.add(key);
    const attributePath = renderPath(path, this.attributeNameSession);

    if (op.type === 'delete') {
      this.removeStatements.push(attributePath);
    } else if (op.type === 'set') {
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.value);
      // if this is a nested path, ensure the top level exist before setting the value. Otherwise, it will throw
      // ValidationException: The document path provided in the update expression is invalid for update.
      this.setStatements.push(`${attributePath} = ${valueIdentifier}`);
    } else if (op.type === 'set_if_not_exists') {
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.value);
      this.setStatements.push(`${attributePath} = if_not_exists(${attributePath}, ${valueIdentifier})`);
    } else if (op.type === 'increment') {
      let operand = attributePath;
      if (op.initialValue !== undefined) {
        const initialValueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.initialValue);
        operand = `if_not_exists(${attributePath}, ${initialValueIdentifier})`;
      }
      const deltaIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.delta);
      this.setStatements.push(`${attributePath} = ${operand} + ${deltaIdentifier}`);
    } else if (op.type === 'list_append') {
      const value = Array.isArray(op.value) ? op.value : [op.value];
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(value);
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
    } else if (op.type === 'add_to_set') {
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.value);
      this.addStatements.push(`${attributePath} ${valueIdentifier}`);
    } else if (op.type === 'delete_from_set') {
      const valueIdentifier = this.attributeValueSession.provideAttributeValueIdentifier(op.value);
      this.deleteFromSetStatements.push(`${attributePath} ${valueIdentifier}`);
    }
    return this;
  }

  hasUpdate() {
    return this.removeStatements.length > 0 || this.setStatements.length > 0 || this.addStatements.length > 0 || this.deleteFromSetStatements.length > 0;
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
    if (this.addStatements.length > 0) {
      statements.push(`ADD ${this.addStatements.join(', ')}`);
    }
    if (this.deleteFromSetStatements.length > 0) {
      statements.push(`DELETE ${this.deleteFromSetStatements.join(', ')}`);
    }

    return {
      updateExpression: statements.join(' '),
      expressionAttributeValues: this.attributeValueSession.expressionAttributeValues,
      expressionAttributeNames: this.attributeNameSession.expressionAttributeNames,
    };
  }
}
