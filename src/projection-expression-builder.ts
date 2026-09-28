import { AttributePath, pathKey, renderPath } from './attribute-path';
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

  get(path: AttributePath): ProjectionExpressionBuilder {
    const key = pathKey(path);
    if (this.visitedPaths.has(key)) {
      // already required.
      return this;
    }
    this.visitedPaths.add(key);
    this.expressions.push(renderPath(path, this.attributeNameSession));
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
