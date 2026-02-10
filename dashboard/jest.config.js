module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    // This ensures Jest looks for files in your src directory
    roots: ['<rootDir>/src'],
    // This maps the generated flatbuffers code if it uses imports
    moduleNameMapper: {
        // This is the magic line:
        // It redirects imports from './foo.js' to './foo.ts'
        '^(\\.\\.?\\/.+)\\.js$': '$1',
    },
};