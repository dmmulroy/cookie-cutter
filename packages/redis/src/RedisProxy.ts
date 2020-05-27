/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

import {
    DefaultComponentContext,
    IComponentContext,
    IDisposable,
    ILogger,
    IRequireInitialization,
} from "@walmartlabs/cookie-cutter-core";
import { ClientOpts, RedisClient, Callback } from "redis";
import { promisify } from "util";

export enum RedisLogMessages {
    Connected = "Redis Connected",
    Error = "Redis Error",
    Ready = "Redis Ready",
    Reconnecting = "Redis Reconnecting",
    End = "Redis End",
}

export enum RedisEvents {
    Connected = "connected",
    Error = "error",
    Ready = "ready",
    Reconnecting = "reconnecting",
    End = "end",
}

// [[streamName, [[streamId, [key, value, key, value ...]]]]]
export type StreamResult = [[string, [[string, string[]]]]];

interface IRedisCommandPatches {
    xadd: (key: string, id: string, ...args: (string | Buffer)[] | Callback<string>[]) => boolean;
    xread: (args: string[], cb: Callback<StreamResult>) => boolean;
    xreadgroup: (args: string[], cb: Callback<StreamResult>) => boolean;
    xgroup: (args: string[], cb: Callback<"OK">) => boolean;
    xack: (args: string[], cb: Callback<number>) => boolean;
    xpending: (args: string[], cb: Callback<[[string, string, number, number]]>) => boolean;
    xclaim: (args: string[], cb: Callback<StreamResult>) => boolean;
    set: (key: string, value: string | Buffer, cb: Callback<"OK">) => boolean;
}

export type RedisClientWithStreamOperations = RedisClient & IRedisCommandPatches;

export type RedisOptionArray = [string, string];

export class RedisProxy implements IRequireInitialization, IDisposable {
    private client: RedisClientWithStreamOperations;
    private logger: ILogger;
    private asyncGet: (key: string) => Promise<string>;
    private asyncSet: (key: string, value: string | Buffer) => Promise<"OK">;
    private asyncQuit: () => Promise<any>;
    private asyncXAdd: (
        streamName: string,
        id: string,
        ...keyValues: (string | Buffer)[]
    ) => Promise<string>;
    private asyncXRead: (args: string[]) => Promise<StreamResult>;
    private asyncXReadGroup: (args: string[]) => Promise<StreamResult>;
    private asyncXGroup: (args: string[]) => Promise<"OK">;
    private asyncXAck: (args: string[]) => Promise<number>;
    private asyncXPending: (args: string[]) => Promise<[[string, string, number, number]]>;
    private asyncXClaim: (args: string[]) => Promise<StreamResult>;

    constructor(host: string, port: number, db: number) {
        this.logger = DefaultComponentContext.logger;
        const opts: ClientOpts = {
            host,
            port,
            db,
        };
        // Redis ^2.8.0 includes all of the stream operations available on the the client.
        // However, @types/redis@3.0.2 does not currently include typings of the stream operations.
        // As proof, we can see redis@2.8.0 lists redis-commands@^1.5.0 as a dependency (https://www.runpkg.com/?redis@3.0.2/package.json)
        // If we then look at redis-commands@1.5.0, we can see the available commands (including all stream commands) in the commands.json file (https://www.runpkg.com/?redis-commands@1.5.0/commands.json)
        this.client = new RedisClient(opts) as RedisClientWithStreamOperations;
        this.client.on(RedisEvents.Connected, () => {
            this.logger.info(RedisLogMessages.Connected);
        });
        this.client.on(RedisEvents.Error, (err) => {
            this.logger.error(RedisLogMessages.Error, err);
            throw err;
        });
        this.client.on(RedisEvents.Ready, () => {
            this.logger.info(RedisLogMessages.Ready);
        });
        this.client.on(RedisEvents.Reconnecting, () => {
            this.logger.info(RedisLogMessages.Reconnecting);
        });
        this.client.on(RedisEvents.End, () => {
            this.logger.info(RedisLogMessages.End);
        });

        this.asyncGet = promisify(this.client.get).bind(this.client);
        this.asyncSet = promisify(this.client.set).bind(this.client);
        this.asyncXAdd = promisify(this.client.xadd).bind(this.client);
        this.asyncXRead = promisify(this.client.xread).bind(this.client);
        this.asyncXReadGroup = promisify(this.client.xreadgroup).bind(this.client);
        this.asyncXGroup = promisify(this.client.xgroup).bind(this.client);
        this.asyncXAck = promisify(this.client.xack).bind(this.client);
        this.asyncXPending = promisify(this.client.xpending).bind(this.client);
        this.asyncQuit = promisify(this.client.quit).bind(this.client);
    }

    public async initialize(ctx: IComponentContext) {
        this.logger = ctx.logger;
    }

    public async dispose() {
        return this.asyncQuit();
    }

    public async set(key: string, value: string | Buffer) {
        return this.asyncSet(key, value);
    }

    public async get(key: string): Promise<string | undefined> {
        return this.asyncGet(key);
    }

    public async xadd(
        streamName: string,
        id: string,
        ...args: (string | Buffer)[]
    ): Promise<string> {
        return this.asyncXAdd(streamName, id, ...args);
    }

    public async xread(args: string[]): Promise<StreamResult> {
        return this.asyncXRead(args);
    }

    public async xgroup(args: string[]): Promise<"OK"> {
        return this.asyncXGroup(args);
    }

    public async xack(
        streamName: string,
        consumerGroup: string,
        streamId: string
    ): Promise<number> {
        return this.asyncXAck([streamName, consumerGroup, streamId]);
    }

    public async xreadgroup(args: string[]): Promise<StreamResult> {
        return this.asyncXReadGroup(args);
    }

    public async xpending(args: string[]): Promise<[[string, string, number, number]]> {
        return this.asyncXPending(args);
    }

    public async xclaim(args: string[]): Promise<StreamResult> {
        return this.asyncXClaim(args);
    }
}
