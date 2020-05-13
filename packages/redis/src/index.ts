/*
Copyright (c) Walmart Inc.

This source code is licensed under the Apache 2.0 license found in the
LICENSE file in the root directory of this source tree.
*/

import {
    config,
    IClassType,
    IMessageEncoder,
    IMessageTypeMapper,
} from "@walmartlabs/cookie-cutter-core";
import { SpanContext } from "opentracing";
import { RedisOptions } from "./config";
import { RedisClient } from "./RedisClient";

export interface IRedisOptions {
    readonly host: string;
    readonly port: number;
    readonly db: number;
    readonly encoder: IMessageEncoder;
    readonly typeMapper: IMessageTypeMapper;
}

export type IRedisInputStreamOptions = IRedisOptions & {
    readStream: string;
};

export type IRedisOutputStreamOptions = IRedisOptions & {
    writeStream: string;
};

export enum RedisMetadata {
    OutputSinkStreamKey = "redis.stream.key",
}

export const AutoGenerateRedisStreamID = "*";

export interface IRedisClient {
    putObject<T>(
        context: SpanContext,
        type: string | IClassType<T>,
        body: T,
        key: string
    ): Promise<void>;
    getObject<T>(
        context: SpanContext,
        type: string | IClassType<T>,
        key: string
    ): Promise<T | undefined>;
    xAddObject<T>(
        context: SpanContext,
        type: string | IClassType<T>,
        streamName: string,
        key: string,
        body: T,
        id?: string
    ): Promise<string>;
    xReadObject<T>(
        context: SpanContext,
        type: string | IClassType<T>,
        streamName: string,
        id?: string
    ): Promise<[string, T] | undefined>; // [streamId, T]
}

export function redisClient(configuration: IRedisOptions): IRedisClient {
    configuration = config.parse(RedisOptions, configuration);
    return new RedisClient(configuration);
}
