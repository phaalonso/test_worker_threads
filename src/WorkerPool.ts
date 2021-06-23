import { Worker } from 'worker_threads';
import { AsyncResource } from "async_hooks";
import { EventEmitter } from "events";
import path from 'path';

const kTaskInfo = Symbol('kTaskInfo');
const kWorkerFreedEvent = Symbol('kWorkerFreedEvent')

class WorkerPoolTaskInfo extends AsyncResource {
    private callback;

    constructor(callback) {
        super('WorkerPoolTaskInfo');

        this.callback = callback;
    }

    public done(err: Error, result: any) {
        this.runInAsyncScope(this.callback, null, err, result);
        this.emitDestroy();
    }
}

export class WorkerPool extends EventEmitter {
    private numThreads: number;
    private workers: Worker[];
    private freeWorkers: Worker[];
    private tasks;

    constructor(numThreads: number) {
        super();

        this.numThreads = numThreads;
        this.workers = [];
        this.freeWorkers = [];
        this.tasks = [];

        for (let i = 0; i < this.numThreads; i++) {
            this.addNewWorker();
        }

        this.on(kWorkerFreedEvent, () => {
            if (this.tasks.length > 0) {
                const { task, callback } = this.tasks.shift();
                this.runTask(task, callback);
            }
        });
    }

    public addNewWorker() {
        const worker = new Worker(path.resolve(__dirname, './task.js'));

        worker.on('message', result => {
            // Em caso de sucesso, chama a callback que foi passada para `runTask`
            // remove `TaskInfo` associado com o Worker e o demarca como free novamente
            worker[kTaskInfo].done(null, result);
            worker[kTaskInfo] = null;
            this.freeWorkers.push(worker);
            this.emit(kWorkerFreedEvent);
        })
    
        worker.on('error', err => {
            // No caso de uma exceção não tratada, chama a callback que foi passada 
            // a `runTask` com o erro
            if (worker[kTaskInfo])
                worker[kTaskInfo].done(err, null);
            else
                this.emit('error', err);

            // Remove o worker da lista e inicia um novo worker que o subistituirá
            this.workers.splice(this.workers.indexOf(worker), 1);
            this.addNewWorker();
        });

        this.workers.push(worker);
        this.freeWorkers.push(worker);
        this.emit('kWorkerFreedEvent');
    }

    public runTask(task, callback) {
        if (this.freeWorkers.length === 0) {
            // Nenhuma thread desocupada, irá esoerar até que alguma tarefa seja finalizada
            this.tasks.push({ task, callback });
            return;
        }

        const worker = this.freeWorkers.pop();
        worker[kTaskInfo] = new WorkerPoolTaskInfo(callback);
        worker.postMessage(task)
    }

    close() {
        for (const worker of this.workers) {
            worker.terminate();
        }
    }
}