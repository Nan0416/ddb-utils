import { AttributeNameSession } from './attribute-session';
import { InvalidDynamoDbProjectionRequestError } from './errors';

export interface ProjectionExpression {
  readonly projectionExpression: string;
  readonly expressionAttributeNames: Record<string, string>;
}

export class ProjectionExpressionBuilder {
  private readonly expressions: string[];
  private readonly attributeNameSession: AttributeNameSession;
  private visitedPaths: Set<string>;

  constructor(attributeNameSession?: AttributeNameSession) {
    this.expressions = [];
    this.attributeNameSession = attributeNameSession ?? new AttributeNameSession();
    this.visitedPaths = new Set();
  }

  get(path: string | ReadonlyArray<string>): ProjectionExpressionBuilder {
    if (typeof path === 'string') {
      path = [path];
    }

    const pathIdentifier = path.join('.');

    if (this.visitedPaths.has(pathIdentifier)) {
      // already required.
      return this;
    }
    this.visitedPaths.add(pathIdentifier);
    const attributeNameIdentifiers: string[] = [];
    path.forEach((segment) => {
      const attributeNameIdentifier = this.attributeNameSession.provideAttributeNameIdentifier(segment);
      attributeNameIdentifiers.push(attributeNameIdentifier);
    });

    this.expressions.push(attributeNameIdentifiers.join('.'));
    return this;
  }

  hasProjection() {
    return this.expressions.length > 0;
  }

  build(): ProjectionExpression {
    if (!this.hasProjection()) {
      throw new InvalidDynamoDbProjectionRequestError("Projection request can't be empty.");
    }

    return {
      projectionExpression: this.expressions.join(', '),
      expressionAttributeNames: this.attributeNameSession.expressionAttributeNames,
    };
  }
}
