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
    "/node_modules/",
    "src/index\\.ts$",
  ],
  coverageThreshold: {
    global: {
      statements: 95,
      branches: 90,
      functions: 100,
      lines: 95,
    },
  },
  reporters: [
    'default',
    ['jest-junit', {
      suiteName: 'ddb-utils-unittest',
      outputDirectory: 'reports/test-reports',
      outputName: 'junit.xml'
    }]
  ],
  testTimeout: 5_000,
}
