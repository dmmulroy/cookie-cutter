import {
    IInputSource,
    IRequireInitialization,
    IDisposable,
    MessageRef,
    IComponentContext,
    DefaultComponentContext,
    Lifecycle,
    makeLifecycle,
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
        let currentId = "$";
        while (!this.done) {
            const span = this.tracer.startSpan(this.spanOperationName);

            this.spanLogAndSetTags(span, this.config.db, this.config.readStream);

            const [streamId, msg] = await this.client.xReadObject<MessageRef>(
                span.context(),
                MessageRef.name,
                this.config.readStream,
                currentId
            );

            currentId = streamId;

            yield msg;

            span.finish();
        }
    }

    stop(): Promise<void> {
        throw new Error("Method not implemented.");
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

        // Attempt to create stream + consumer group if they don't already exist
        await this.client.xGroup(span.context(), this.config.readStream, this.config.consumerGroup);

        span.finish();
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
