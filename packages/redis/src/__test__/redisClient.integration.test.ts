/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

import {
    DefaultComponentContext,
    IMessage,
    JsonMessageEncoder,
    Lifecycle,
    makeLifecycle,
    ObjectNameMessageTypeMapper,
    Application,
    StaticInputSource,
    IDispatchContext,
    ConsoleLogger,
    ErrorHandlingMode,
    timeout,
} from "@walmartlabs/cookie-cutter-core";
import { SpanContext } from "opentracing";
import { createClient } from "redis";

import {
    IRedisClient,
    redisClient,
    IRedisOutputStreamOptions,
    IRedisInputStreamOptions,
} from "../index";
import { RedisStreamSink } from "../RedisStreamSink";
import { promisify } from "util";
import { RedisClientWithStreamOperations, StreamResult } from "../RedisProxy";
import { RedisStreamSource } from "../RedisStreamSource";

class Foo {
    constructor(public text: string) {}
}

class Bar {
    constructor(public text: string) {}
}
class TestClass {
    constructor(public contents: string) {}
}

describe("redis integration test", () => {
    const config: IRedisOutputStreamOptions & IRedisInputStreamOptions = {
        host: "localhost",
        port: 6379,
        db: 0,
        encoder: new JsonMessageEncoder(),
        typeMapper: new ObjectNameMessageTypeMapper(),
        writeStream: "test-stream",
        readStream: "test-stream",
        consumerGroup: "test-consumer-group",
        idleTimeoutBatchSize: 5,
        idleTimeoutMs: 5000,
    };
    let ccClient: Lifecycle<IRedisClient>;
    let client: RedisClientWithStreamOperations;
    let asyncXRead;
    let asyncFlushAll;
    let asyncQuit;

    beforeAll(async () => {
        ccClient = makeLifecycle(redisClient(config));
        await ccClient.initialize(DefaultComponentContext);

        client = createClient(config.port, config.host) as RedisClientWithStreamOperations;
        asyncXRead = promisify(client.xread).bind(client);
        asyncFlushAll = promisify(client.flushall).bind(client);
        asyncQuit = promisify(client.quit).bind(client);
    });

    afterEach(async () => {
        return await asyncFlushAll();
    });

    afterAll(async () => {
        await Promise.all([ccClient.dispose(), asyncQuit()]);
    });

    it("does not get a value for an non-existing key", async () => {
        const aKey = "key1";
        expect(await ccClient.getObject(new SpanContext(), Uint8Array, aKey)).toBeUndefined();
    });

    it("successfully sets and gets a value for a given key", async () => {
        const span = new SpanContext();
        const aKey = "key2";
        const msg: IMessage = {
            type: TestClass.name,
            payload: new TestClass("test contents"),
        };
        expect(await ccClient.putObject(span, TestClass, msg.payload, aKey)).toBeUndefined();
        const outputPayload = await ccClient.getObject(span, TestClass, aKey);
        expect(outputPayload).toMatchObject(msg.payload);
    });

    it("RedisClient successfully xadds a value into a stream and returns the id", async () => {
        const span = new SpanContext();
        const key = "test";
        const value: IMessage = {
            type: TestClass.name,
            payload: new TestClass("test"),
        };

        const id = await ccClient.xAddObject(span, TestClass.name, "test-stream", key, value);

        expect(id).not.toBeFalsy();
    });

    it("successfully adds a value to a redis stream through the output sink", async () => {
        const inputMsg = new Foo("test");
        const app = Application.create()
            .logger(new ConsoleLogger())
            .input()
            .add(new StaticInputSource([{ type: Foo.name, payload: inputMsg }]))
            .done()
            .dispatch({
                onFoo: async (msg: Foo, ctx: IDispatchContext) => {
                    ctx.publish(Bar, new Bar(`output for ${msg.text}`));
                },
            })
            .output()
            .published(new RedisStreamSink(config))
            .done()
            .run(ErrorHandlingMode.LogAndContinue);

        try {
            await timeout(app, 5000);
        } catch (error) {
            app.cancel();
        } finally {
            const results = await asyncXRead(["streams", "test-stream", "0"]);
            expect(results).not.toBeFalsy();

            // This IIFE will be moved into a utility function in the PR for the Stream Source
            // and refactored/used here in that PR.
            const storedValue = ((results: StreamResult): Uint8Array => {
                // Since our initial implementation on pulls 1 value from 1 stream at a time
                // there should only 1 item in results

                // [streamName, [streamValue]]
                const [, [streamValue]] = results[0];

                // [streamId, keyValues]
                const [, keyValues] = streamValue;

                // [RedisMetadata.OutputSinkStreamKey, data]
                const [, data] = keyValues;

                return new Uint8Array(Buffer.from(data, "base64"));
            })(results);

            const msg = config.encoder.decode(storedValue, Bar.name);

            expect(msg.payload.text).toEqual(`output for ${inputMsg.text}`);
        }
    });

    it("succesfully creates a new consumer group", async () => {
        const app = Application.create()
            .logger(new ConsoleLogger())
            .input()
            .add(new RedisStreamSource(config))
            .done()
            .run(ErrorHandlingMode.LogAndContinue);
    });
});
