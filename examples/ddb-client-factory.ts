import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { AwsCredentialIdentityProvider, AwsCredentialIdentity } from '@smithy/types';

export function buildDdbClient(props: { region: string; credentials: AwsCredentialIdentity | AwsCredentialIdentityProvider }) {
  return DynamoDBDocument.from(
    new DynamoDB({
      region: props.region,
      credentials: props.credentials,
    }),
    {
      marshallOptions: {
        /**
         * DDB doesn't have a native type that is corresponding to JavaScript undefined.
         * Use the option to drop the javascript object undefined properties.
         */
        removeUndefinedValues: true,
      },
      unmarshallOptions: {},
    },
  );
}
