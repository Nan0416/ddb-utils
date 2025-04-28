/**
 * @type {import('@jest/types').Config.InitialOptions}
 */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  passWithNoTests: false,
  testMatch: [
    "<rootDir>/tests/**/*.test.(js|ts)"
  ],
  collectCoverage: true,
  coverageDirectory: 'reports/coverage-reports',
  collectCoverageFrom: [
    "src/**/*.(ts|js)",
  ],
  coveragePathIgnorePatterns: [
    "index.(ts|js)",
  ],
  reporters: [
    'default',
    ['jest-junit', {
      suiteName: 'ddb-utils-unittest',
      outputDirectory: 'reports/test-reports',
      outputName: 'junit.xml'
    }]
  ],
  testTimeout: 10_000,
}