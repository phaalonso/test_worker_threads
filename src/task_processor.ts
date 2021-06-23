import { parentPort } from 'worker_threads';

parentPort.on('message', task => {
    parentPort.postMessage(task.a + task.b);
});