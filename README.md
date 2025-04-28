# ddb-utils

3. Update jest.config.js suiteName to package name.

![release workflow](https://github.com/Nan0416/ddb-utils/actions/workflows/release.yml/badge.svg)

![Latest PR workflow](https://github.com/Nan0416/ddb-utils/actions/workflows/pr.yml/badge.svg)

[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)


## Setup Permission

The goal is to set up a github workflow that automaitcally builds and publishes the library artifact to the public npm repo when the code is merged into "main" branch. 

You don't need to configure permission to download package. However, to publish package, you will need to manually create an npm token, and save it into the github action.

### Step 1. Create NPM token.

1. Go to https://www.npmjs.com/settings/{username}/tokens, choose "Classic Token", and select "Automation". Take a note on the generate token.

> To make it easy manage, name the token as "For {git repo name}".

2. Go to the github repo, "Settings" > "Secrets and variables" > "Actions" > Click "New repositoty secret". Set token name as "NPM_TOKEN" and paste the token generated from npm. 
