import azure from "azure-storage";

declare namespace Consumer {
    export type ConsumerDone = (error?: Error) => void;

    export interface Options {
        queueName: string;
        handleMessage(message: azure.QueueService.Message, done: ConsumerDone): any;
        batchSize?: number;
        waitTimeSeconds?: number;
        authenticationErrorTimeout?: number;
        queueService?: azure.QueueService;
    }
}

declare class Consumer extends NodeJS.EventEmitter {
    constructor(options: Consumer.Options);
    start(): void;
    stop(): void;
    static create(options: Consumer.Options): Consumer;
}

export = Consumer;