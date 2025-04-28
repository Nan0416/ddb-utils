import { AttributeSession } from './attribute-session';

export interface Condition {
  readonly expression: string;
}

export class ConditionExpressionBuilder {
  private readonly attributeManager: AttributeSession;

  constructor(attributeManager: AttributeSession) {
    this.attributeManager = attributeManager;
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
      const attributeNameIdentifier = this.attributeManager.provideAttributeNameIdentifier(segment);
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

    const valueIdentifier = this.attributeManager.provideAttributeValueIdentifier(value);
    const attributeNameIdentifiers: string[] = [];
    segments.forEach((segment) => {
      const attributeNameIdentifier = this.attributeManager.provideAttributeNameIdentifier(segment);
      attributeNameIdentifiers.push(attributeNameIdentifier);
    });

    return {
      expression: `${attributeNameIdentifiers.join('.')} = ${valueIdentifier}`,
    };
  }
}
