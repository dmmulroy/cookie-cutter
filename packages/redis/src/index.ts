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
    IOutputSink,
    IPublishedMessage,
} from "@walmartlabs/cookie-cutter-core";
import { SpanContext } from "opentracing";
import { RedisOptions } from "./config";
import { RedisClient } from "./RedisClient";
import { RedisStreamSink } from "./RedisStreamSink";

export interface IRedisOptions {
    readonly host: string;
    readonly port: number;
    readonly db: number;
    readonly encoder: IMessageEncoder;
    readonly typeMapper: IMessageTypeMapper;
    readonly base64Encode?: boolean;
}

export type IRedisInputStreamOptions = IRedisOptions & {
    readStreams: string[];
};

export type IRedisOutputStreamOptions = IRedisOptions & {
    writeStream: string;
};

export enum RedisMetadata {
    OutputSinkStreamKey = "redis.stream.key",
}

export enum RedisStreamID {
    AutoGenerate = "*",
}

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
}

export function redisClient(configuration: IRedisOptions): IRedisClient {
    configuration = config.parse(RedisOptions, configuration, { base64Encode: true });
    return new RedisClient(configuration);
}

export function redisStreamSink(
    configuration: IRedisOutputStreamOptions
): IOutputSink<IPublishedMessage> {
    configuration = config.parse(RedisOptions, configuration, { base64Encode: true });
    return new RedisStreamSink(configuration);
}
