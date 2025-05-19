import { AttributeNameSession, AttributeValueSession } from './attribute-session';

export interface Condition {
  readonly expression: string;
}

export class ConditionExpressionBuilder {
  private readonly attributeNameManager: AttributeNameSession;
  private readonly attributeValueManager: AttributeValueSession;

  constructor(attributeNameManager: AttributeNameSession, attributeValueManager: AttributeValueSession) {
    this.attributeNameManager = attributeNameManager;
    this.attributeValueManager = attributeValueManager;
  }

  attribute_exists(path: string | string[]): Condition {
    let segments: ReadonlyArray<string>;
    if (typeof path === 'string') {
      segments = [path];
    } else {
      segments = path;
    }
    const attributeNameIdentifiers: string[] = [];
    segments.forEach((segment) => {
      const attributeNameIdentifier = this.attributeNameManager.provideAttributeNameIdentifier(segment);
      attributeNameIdentifiers.push(attributeNameIdentifier);
    });
    return {
      expression: `attribute_exists(${attributeNameIdentifiers.join('.')})`,
    };
  }

  and(...conditions: Condition[]): Condition {
    return {
      expression: conditions.map((condition) => `(${condition.expression})`).join(' AND '),
    };
  }

  equal(path: string | ReadonlyArray<string>, value: string | number | boolean): Condition {
    let segments: ReadonlyArray<string>;
    if (typeof path === 'string') {
      segments = [path];
    } else {
      segments = path;
    }

    const valueIdentifier = this.attributeValueManager.provideAttributeValueIdentifier(value);
    const attributeNameIdentifiers: string[] = [];
    segments.forEach((segment) => {
      const attributeNameIdentifier = this.attributeNameManager.provideAttributeNameIdentifier(segment);
      attributeNameIdentifiers.push(attributeNameIdentifier);
    });

    return {
      expression: `${attributeNameIdentifiers.join('.')} = ${valueIdentifier}`,
    };
  }
}
