import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";

const TOPIC_NAME = "quickstart-events";
const client = new PrismaClient();

const kafka = new Kafka({
    clientId: "outbox proccessor",
    brokers: ["localhost:9092"],
  });

async function main() {
    const producer = kafka.producer();
    await producer.connect();

    while (true) {
        const pendingRows = await client.zapRunOutbox.findMany({
            where: {},
            take: 10,
        });
        console.log(pendingRows);

        await producer.send({
            topic: TOPIC_NAME,
            messages: pendingRows.map(r => ({
                value: JSON.stringify({ zapRunId: r.zapRunId, stage: 0 }),
            })),
        });

        await client.zapRunOutbox.deleteMany({
            where: {
                id: {
                    in: pendingRows.map(x => x.id),
                },
            },
        });

        await new Promise(r => setTimeout(r, 3000));
    }
}

main().catch(e => {
    console.error(`Error in main function: ${e.message}`);
    process.exit(1);
});