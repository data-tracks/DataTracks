import * as flatbuffers from 'flatbuffers';
import {DataModel as fb} from '../../generated/value';


export type Value =
    | { type: 'Int'; value: bigint }
    | { type: 'Float'; number: bigint; shift: number }
    | { type: 'Bool'; value: boolean }
    | { type: 'Text'; value: string }
    | { type: 'Time'; ms: bigint; ns: number }
    | { type: 'Date'; days: bigint }
    | { type: 'Array'; values: Value[] }
    | { type: 'Dict'; entries: Record<string, Value> }
    | { type: 'Node'; id: bigint; labels: string[]; properties: Record<string, Value> }
    | { type: 'Edge'; id: bigint; label: string; startId: bigint; endId: bigint; properties: Record<string, Value> }
    | { type: 'Null' };

// Helper types for specific structures if you need to pass them around separately
export interface Node {
    id: bigint;
    labels: string[];
    properties: Record<string, Value>;
}

export interface Edge {
    id: bigint;
    label: string;
    startId: bigint;
    endId: bigint;
    properties: Record<string, Value>;
}

export class ValueMapper {
    /**
     * SERIALIZE:
     * TS Object -> FlatBuffers Bytes
     */
    static pack(val: Value, topics: string[] = [], timestamp: number = Date.now()): Uint8Array {
        const builder = new flatbuffers.Builder(1024);

        // 1. Prepare the payload (the recursive Value)
        const payloadOffset = ValueMapper.toOffset(builder, val);

        // 2. Prepare the topics vector (strings must be created first)
        let topicsOffset = 0;
        if (topics.length > 0) {
            const topicStrings = topics.map(t => builder.createString(t));
            topicsOffset = fb.Message.createTopicsVector(builder, topicStrings);
        }

        // 3. Build the Message table
        fb.Message.startMessage(builder);

        // Add the payload union offset
        fb.Message.addPayload(builder, payloadOffset);

        // Add the timestamp (longs are handled as BigInt or two 32-bit ints depending on your library version)
        fb.Message.addTimestamp(builder, BigInt(timestamp));

        // Add the topics vector offset
        if (topicsOffset !== 0) {
            fb.Message.addTopics(builder, topicsOffset);
        }

        const messageOffset = fb.Message.endMessage(builder);

        // 4. Finish the buffer
        fb.Message.finishMessageBuffer(builder, messageOffset);

        return builder.asUint8Array();
    }

    private static toOffset(builder: flatbuffers.Builder, val: Value): flatbuffers.Offset {
        let dataType = fb.ValueData.NONE;
        let dataOffset = 0;

        switch (val.type) {
            case 'Int':
                dataType = fb.ValueData.Int;
                dataOffset = fb.Int.createInt(builder, val.value);
                break;

            case 'Float':
                dataType = fb.ValueData.Float;
                dataOffset = fb.Float.createFloat(builder, val.number, val.shift);
                break;

            case 'Bool':
                dataType = fb.ValueData.Bool;
                dataOffset = fb.Bool.createBool(builder, val.value);
                break;

            case 'Text':
                const strOff = builder.createString(val.value);
                dataType = fb.ValueData.Text;
                dataOffset = fb.Text.createText(builder, strOff);
                break;

            case 'Time':
                dataType = fb.ValueData.Time;
                dataOffset = fb.Time.createTime(builder, val.ms, val.ns);
                break;

            case 'Date':
                dataType = fb.ValueData.Date;
                dataOffset = fb.Date.createDate(builder, val.days);
                break;

            case 'Array':
                // Recursively get offsets for each Value table in the array
                const childOffsets = val.values.map(v => ValueMapper.toOffset(builder, v));
                const vecOff = fb.Array.createValuesVector(builder, childOffsets);
                dataType = fb.ValueData.Array;
                dataOffset = fb.Array.createArray(builder, vecOff);
                break;

            case 'Dict':
                const dictEntryOffsets = Object.entries(val.entries).map(([k, v]) => {
                    const kOff = builder.createString(k);
                    const vOff = ValueMapper.toOffset(builder, v);
                    fb.DictEntry.startDictEntry(builder);
                    fb.DictEntry.addKey(builder, kOff);
                    fb.DictEntry.addValue(builder, vOff);
                    return fb.DictEntry.endDictEntry(builder);
                });
                const dictVecOff = fb.Dict.createEntriesVector(builder, dictEntryOffsets);
                dataType = fb.ValueData.Dict;
                dataOffset = fb.Dict.createDict(builder, dictVecOff);
                break;

            case 'Node':
                const labelOffsets = val.labels.map(l => builder.createString(l));
                const labelsVec = fb.Node.createLabelsVector(builder, labelOffsets);
                const nodePropOffsets = Object.entries(val.properties).map(([k, v]) => {
                    const kOff = builder.createString(k);
                    const vOff = ValueMapper.toOffset(builder, v);
                    fb.DictEntry.startDictEntry(builder);
                    fb.DictEntry.addKey(builder, kOff);
                    fb.DictEntry.addValue(builder, vOff);
                    return fb.DictEntry.endDictEntry(builder);
                });
                const nodePropsVec = fb.Node.createPropertiesVector(builder, nodePropOffsets);
                dataType = fb.ValueData.Node;
                dataOffset = fb.Node.createNode(builder, val.id, labelsVec, nodePropsVec);
                break;

            case 'Edge':
                const edgeLabelOff = builder.createString(val.label);
                const edgePropOffsets = Object.entries(val.properties).map(([k, v]) => {
                    const kOff = builder.createString(k);
                    const vOff = ValueMapper.toOffset(builder, v);
                    fb.DictEntry.startDictEntry(builder);
                    fb.DictEntry.addKey(builder, kOff);
                    fb.DictEntry.addValue(builder, vOff);
                    return fb.DictEntry.endDictEntry(builder);
                });
                const edgePropsVec = fb.Edge.createPropertiesVector(builder, edgePropOffsets);
                dataType = fb.ValueData.Edge;
                dataOffset = fb.Edge.createEdge(builder, val.id, edgeLabelOff, val.startId, val.endId, edgePropsVec);
                break;

            case 'Null':
                dataType = fb.ValueData.NONE;
                dataOffset = 0;
                break;
        }

        // Final wrap into the Value table
        fb.Value.startValue(builder);
        fb.Value.addDataType(builder, dataType);
        if (dataType !== fb.ValueData.NONE) {
            fb.Value.addData(builder, dataOffset);
        }
        return fb.Value.endValue(builder);

    }

    /**
     * DESERIALIZE: FlatBuffers Bytes -> TS Value Object
     */
    static unpack(bytes: Uint8Array): Value {
        const buf = new flatbuffers.ByteBuffer(bytes);
        const fbMsg = fb.Message.getRootAsMessage(buf);
        //const fbVal = fb.Value.getRootAsValue(buf);
        const fbVal = fbMsg.payload();
        if (fbVal){
            return ValueMapper.fromFB(fbVal);
        }
        return { type: 'Null' }

    }

    private static fromFB(fbVal: fb.Value): Value {
        const type = fbVal.dataType();

        switch (type) {
            case fb.ValueData.Int: {
                const table = fbVal.data(new fb.Int())!;
                return { type: 'Int', value: table.value() };
            }

            case fb.ValueData.Float: {
                const table = fbVal.data(new fb.Float())!;
                return { type: 'Float', number: table.number(), shift: table.shift() };
            }

            case fb.ValueData.Bool: {
                const table = fbVal.data(new fb.Bool())!;
                return { type: 'Bool', value: table.value() };
            }

            case fb.ValueData.Text: {
                const table = fbVal.data(new fb.Text())!;
                return { type: 'Text', value: table.value() || "" };
            }

            case fb.ValueData.Time: {
                const table = fbVal.data(new fb.Time())!;
                return { type: 'Time', ms: table.ms(), ns: table.ns() };
            }

            case fb.ValueData.Date: {
                const table = fbVal.data(new fb.Date())!;
                return { type: 'Date', days: table.days() };
            }

            case fb.ValueData.Array: {
                const table = fbVal.data(new fb.Array())!;
                const values: Value[] = [];
                const len = table.valuesLength();
                for (let i = 0; i < len; i++) {
                    const item = table.values(i);
                    if (item) values.push(ValueMapper.fromFB(item));
                }
                return { type: 'Array', values };
            }

            case fb.ValueData.Dict: {
                const table = fbVal.data(new fb.Dict())!;
                return { type: 'Dict', entries: ValueMapper.unpackProperties(i => table.entries(i), table.entriesLength()) };
            }

            case fb.ValueData.Node: {
                const table = fbVal.data(new fb.Node())!;

                // Unpack labels
                const labels: string[] = [];
                const labelLen = table.labelsLength();
                for (let i = 0; i < labelLen; i++) {
                    const l = table.labels(i);
                    if (l !== null) labels.push(l);
                }

                return {
                    type: 'Node',
                    id: table.id(),
                    labels,
                    properties: ValueMapper.unpackProperties(i => table.properties(i), table.propertiesLength())
                };
            }

            case fb.ValueData.Edge: {
                const table = fbVal.data(new fb.Edge())!;
                return {
                    type: 'Edge',
                    id: table.id(),
                    label: table.label() || "",
                    startId: table.startId(),
                    endId: table.endId(),
                    properties: ValueMapper.unpackProperties(i => table.properties(i), table.propertiesLength())
                };
            }

            case fb.ValueData.NONE:
            default:
                return { type: 'Null' };
        }
    }

    /**
     * Helper to convert FlatBuffers properties (DictEntry[]) to a TS Record
     */
    private static unpackProperties(prop: (i:number) => any, length: number): Record<string, Value> {
        const properties: Record<string, Value> = {};
        for (let i = 0; i < length; i++) {
            const entry = prop(i);
            if (entry && entry.key()) {
                const val = entry.value();
                if (val) {
                    properties[entry.key()!] = ValueMapper.fromFB(val);
                }
            }
        }
        return properties;
    }
}