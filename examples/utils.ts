export function getenv(name: string): string {
  const value = process.env[name];

  if (typeof value === 'string') {
    return value;
  }

  throw new Error(`Missing environment variable ${name}`);
}
