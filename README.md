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

## Update actions

| Method | Emits |
|---|---|
| `set(path, value)` | `SET p = :v` |
| `setFields({ a, b, c }, { removeNulls })` | one `set` per field; `undefined` skipped, `null` stored or (with `removeNulls`) removed |
| `setIfNotExists(path, value)` | `SET p = if_not_exists(p, :v)` |
| `increment(path, delta, initialValue?)` | `SET p = p + :d`, or `SET p = if_not_exists(p, :init) + :d` |
| `append(path, value, position?, failIfMissing?)` | `SET p = list_append(...)` |
| `delete(path)` | `REMOVE p` |
| `addToSet(path, value)` / `deleteFromSet(path, value)` | `ADD p :v` / `DELETE p :v` |

## Paths

Every method that takes a path accepts an attribute name, or its segments as an array: a string segment is a map key and a number is a list index. `['notes', 3, 'noteId']` is `#a0[3].#a1`. An index is written into the expression, because DynamoDB accepts no placeholder for one.

## Conditions

`ConditionExpressionBuilder` (and `UpdateExpressionBuilder.conditionExpressionBuilder`, `QueryExpressionBuilder.filterExpressionBuilder`) provides `equal`, `notEqual`, `lessThan`, `lessThanOrEqualTo`, `greaterThan`, `greaterThanOrEqualTo`, `between`, `beginsWith`, `contains`, `in` (1 to 100 values), `attributeExists`, `attributeNotExists`, combined with `and`, `or` and `not`. `size(path)` is a left operand for the comparisons: `cond.equal(cond.size('holds'), 0)` is `size(#a0) = :v0`. Values may be any DynamoDB value, `null` and lists included.

## Reading items

`ItemParser` reads typed attributes and throws `ItemParseError` (with the attribute's `key`) for a missing required attribute or a wrong type. `extractX` requires the attribute, `extractOptionalX` returns `undefined` when it is absent, and `extractNullableX` also returns `null` for a stored NULL.

## How to run examples

1. Define two environment variables. The project uses [dotenv](https://www.npmjs.com/package/dotenv) to load environment variables from a `.env` file.

In the project root directory, create `.env` file with two variables

```
AWS_REGION={YourTableAwsRegion}
# We use SSO to provision aws credentials. If SSO is not enabled in the AWS account, you have to modify the examples code to replace the credentials logic.
# Run `aws configure sso --profile {YourProfileName}` to configure the profile, and run `aws sso login --profile {YourProfileName}` to load credentials.
SSO_PROFILE_NAME={YourProfileName}

# First, create a test table in AWS DDB with partition key column name as "partitionKey" and sort key column name sa "sortKey"
TABLE_NAME={YourTableName}
```