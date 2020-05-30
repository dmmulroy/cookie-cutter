import {
    IInputSource,
    IRequireInitialization,
    IDisposable,
    MessageRef,
    IComponentContext,
    DefaultComponentContext,
    Lifecycle,
    makeLifecycle,
    failSpan,
} from "@walmartlabs/cookie-cutter-core";
import { Span, Tags, Tracer } from "opentracing";

import { IRedisInputStreamOptions, IRedisClient, redisClient } from ".";
import { RedisOpenTracingTagKeys } from "./RedisClient";

export class RedisStreamSource implements IInputSource, IRequireInitialization, IDisposable {
    private done: boolean = false;
    private client: Lifecycle<IRedisClient>;
    private tracer: Tracer;
    private spanOperationName: string = "Redis Input Source Client Call";

    constructor(private readonly config: IRedisInputStreamOptions) {
        this.tracer = DefaultComponentContext.tracer;
    }

    public async *start(): AsyncIterableIterator<MessageRef> {
        let initial = true;
        let pendingMessages;
        while (!this.done) {
            const span = this.tracer.startSpan(this.spanOperationName);

            this.spanLogAndSetTags(
                span,
                this.config.db,
                this.config.readStream,
                this.config.consumerGroup
            );

            try {
                if (initial) {
                    pendingMessages = await this.client.xReadGroupObject<MessageRef>(
                        span.context(),
                        MessageRef.name,
                        this.config.readStream,
                        this.config.consumerGroup,
                        this.config.consumerId,
                        "0"
                    );

                    initial = false;
                } else {
                    pendingMessages = await this.client.xPending(
                        span.context(),
                        this.config.readStream,
                        this.config.consumerGroup,
                        this.config.idleTimeoutBatchSize
                    );
                }

                const expiredIdleMessages = pendingMessages.filter(
                    ({ idleTime }) => idleTime > this.config.idleTimeoutMs
                );

                if (expiredIdleMessages.length > 0) {
                    // drain expired idle messages
                    for (let {
                        streamId,
                        consumerId,
                        idleTime,
                        timesDelivered,
                    } of expiredIdleMessages) {
                        const [, msg] = await this.client.xClaimObject<MessageRef>(
                            span.context(),
                            MessageRef.name,
                            this.config.readStream,
                            this.config.consumerGroup,
                            this.config.consumerId,
                            this.config.idleTimeoutMs,
                            streamId
                        );

                        msg.addMetadata({ streamId, consumerId, idleTime, timesDelivered });

                        msg.once("released", async () => {
                            await this.client.xAck(
                                span.context(),
                                this.config.readStream,
                                this.config.consumerGroup,
                                streamId
                            );

                            span.finish();
                        });

                        yield msg;
                    }
                } else {
                    const [streamId, msg] = await this.client.xReadGroupObject<MessageRef>(
                        span.context(),
                        MessageRef.name,
                        this.config.readStream,
                        this.config.consumerGroup,
                        this.config.consumerId
                    );

                    msg.addMetadata({ streamId });

                    msg.once("released", async () => {
                        await this.client.xAck(
                            span.context(),
                            this.config.readStream,
                            this.config.consumerGroup,
                            streamId
                        );

                        span.finish();
                    });

                    yield msg;
                }
            } catch (error) {
                failSpan(span, error);
                throw error;
            }
        }
    }

    public async *startV2(): AsyncIterableIterator<MessageRef> {
        const span = this.tracer.startSpan(this.spanOperationName);

        this.spanLogAndSetTags(
            span,
            this.config.db,
            this.config.readStream,
            this.config.consumerGroup
        );

        const pendingMessages = await this.client.xReadGroup(
            span.context(),
            this.config.readStream,
            this.config.consumerGroup,
            this.config.consumerId,
            this.config.idleTimeoutBatchSize,
            "0"
        );

        for (const message of pendingMessages) {
            const messageRef = new MessageRef(
                {
                    streamName: this.config.readStream,
                    consumerGroup: this.config.consumerGroup,
                    consumerId: this.config.consumerId,
                },
                message,
                span.context()
            );

            messageRef.once("released", async () => {
                await this.client.xAck(
                    span.context(),
                    this.config.readStream,
                    this.config.consumerGroup,
                    message.streamId
                );

                span.finish();
            });

            yield messageRef;
        }
    }

    stop(): Promise<void> {
        this.done = true;
        return;
    }

    public async initialize(context: IComponentContext): Promise<void> {
        this.tracer = context.tracer;

        this.client = makeLifecycle(redisClient(this.config));
        await this.client.initialize(context);

        const span = this.tracer.startSpan(this.spanOperationName);

        this.spanLogAndSetTags(
            span,
            this.config.db,
            this.config.readStream,
            this.config.consumerGroup
        );

        try {
            // Attempt to create stream + consumer group if they don't already exist
            await this.client.xGroup(
                span.context(),
                this.config.readStream,
                this.config.consumerGroup,
                this.config.consumerGroupStartId
            );
        } catch (error) {
            failSpan(span, error);
            throw error;
        } finally {
            span.finish();
        }
    }

    public async dispose(): Promise<void> {
        await this.client.dispose();
    }

    private spanLogAndSetTags(
        span: Span,
        bucket: number,
        streamName: string,
        consumerGroup: string
    ): void {
        span.log({ bucket, streamName, consumerGroup });

        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_RPC_CLIENT);
        span.setTag(Tags.COMPONENT, "cookie-cutter-redis");
        span.setTag(Tags.DB_INSTANCE, bucket);
        span.setTag(RedisOpenTracingTagKeys.BucketName, bucket);
    }
}
