# ddb-utils

![release workflow](https://github.com/Nan0416/ddb-utils/actions/workflows/release.yml/badge.svg)

![Latest PR workflow](https://github.com/Nan0416/ddb-utils/actions/workflows/pr.yml/badge.svg)

[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)


## Example

```ts
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { UpdateExpressionBuilder } from '@ultrasa/ddb-utils';

const docClient: DynamoDBDocument;
const updateExpressionBuilder = new UpdateExpressionBuilder();

updateExpressionBuilder.set('active', true).set('lastUpdatedAt', '2025-04-28T10:00:00Z').set('version', 'qoe38');
const conditional = updateExpressionBuilder.conditionExpressionBuilder;
const condition = conditional.and(conditional.attribute_exists('accountId'), conditional.equal('version', 'j893w'));
const expression = updateExpressionBuilder.build();

docClient.update({
    TableName: '{YourTablename}',
    Key: {
        'accountId': '123'
    },
    UpdateExpression: expression.updateExpression,
    ExpressionAttributeNames: expression.expressionAttributeNames,
    ExpressionAttributeValues: expression.expressionAttributeValues,
    ConditionExpression: condition.expression
});
```

The update expression sent to DynamoDB is equivalent to 

```JSON
{
    "UpdateExpression": "SET #a0 = :v0, #a1 = :v1, #a2 = :v2",
    "ExpressionAttributeNames": {
        "#a0": "active",
        "#a1": "lastUpdatedAt",
        "#a2": "version",
        "#a3": "accountId"
    },
        "ExpressionAttributeValues": {
        ":v0": true,
        ":v1": "2025-04-28T10:00:00Z",
        ":v2": "qoe38",
        ":v3": "j893w"
    },
    "ConditionExpression": "(attribute_exists(#a3)) AND (#a2 = :v3)"
}
```