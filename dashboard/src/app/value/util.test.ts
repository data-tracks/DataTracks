import { ValueMapper, Value } from './util';

describe('ValueMapper Round-trip Tests', () => {

    test('should serialize and deserialize a complex Node', () => {
        const originalNode: Value = {
            type: 'Node',
            id: BigInt(9007199254740991), // Max safe integer + something to test BigInt
            labels: ['User', 'Admin'],
            properties: {
                'email': { type: 'Text', value: 'dev@gemini.ai' },
                'meta': {
                    type: 'Dict',
                    entries: {
                        'scores': {
                            type: 'Array',
                            values: [
                                { type: 'Int', value: BigInt(10) },
                                { type: 'Float', number: BigInt(1234), shift: 2 } // 12.34
                            ]
                        }
                    }
                }
            }
        };

        // 1. Pack
        const buffer = ValueMapper.pack([originalNode]);
        expect(buffer).toBeInstanceOf(Uint8Array);
        expect(buffer.length).toBeGreaterThan(0);

        // 2. Unpack
        const decoded = ValueMapper.unpack(buffer)[0];

        // 3. Assertions
        expect(decoded).toEqual(originalNode);

        // Specific BigInt check
        if (decoded.type === 'Node') {
            expect(typeof decoded.id).toBe('bigint');
            expect(decoded.id).toBe(BigInt(9007199254740991));
        }
    });

    test('should handle Null values', () => {
        const original: Value = { type: 'Null' };
        const buffer = ValueMapper.pack([original]);
        const decoded = ValueMapper.unpack(buffer)[0];

        expect(decoded).toEqual(original);
    });

    test('should handle Edges correctly', () => {
        const originalEdge: Value = {
            id: BigInt(32),
            type: 'Edge',
            label: 'FOLLOWS',
            startId: BigInt(1),
            endId: BigInt(2),
            properties: {
                'since': { type: 'Date', days: BigInt(12345) }
            }
        };

        const buffer = ValueMapper.pack([originalEdge]);
        const decoded = ValueMapper.unpack(buffer)[0];

        expect(decoded).toEqual(originalEdge);
    });
});