import { Jetstream } from '@skyware/jetstream';
import amqp from 'amqplib';

const POST_QUEUE_NAME = 'POST';
const LIKE_QUEUE_NAME = 'LIKE';

const conn = await amqp.connect('amqp://user:password@messaging');
const channel = await conn.createChannel();

await channel.assertQueue(POST_QUEUE_NAME);

const jetstream = new Jetstream({
    endpoint: 'wss://jetstream1.us-west.bsky.network/subscribe',
    wantedCollections: ["app.bsky.feed.post", "app.bsky.feed.like"],
});


jetstream.on('open', () => {
    console.log('Jetstreamに接続しました');
});

jetstream.on('close', () => {
    console.log('Jetstreamから切断されました');
});

jetstream.on('error', (error) => {
    console.log(`Jetstreamエラーが発生しました: ${error.message}`);
});


jetstream.onCreate("app.bsky.feed.post", (event) => {
    // 言語設定に日本語が含まれる投稿をキューに登録する
    if (event.commit.record.langs?.includes('ja')) {
        // ALTの抽出
        const altList: string[] = [];
        const embed = event.commit.record.embed;
        if (embed !== undefined) {
            switch (embed.$type) {
                case 'app.bsky.embed.images':
                    embed.images
                        .filter(image => image.alt.length !== 0)
                        .forEach(image => altList.push(image.alt));
                    break;
                case 'app.bsky.embed.video':
                    if (embed.alt !== undefined && embed.alt.length !== 0) {
                        altList.push(embed.alt);
                    }
                    break;
                default:
                // 何もしない
            }
        }
        const message = JSON.stringify({
            operation: event.commit.operation,
            uri: createUri(event.did, event.commit.collection, event.commit.rkey),
            cid: event.commit.cid,
            replyParent: event.commit.record.reply?.parent,
            replyRoot: event.commit.record.reply?.root,
            text: event.commit.record.text,
            altList,
        });
        channel.sendToQueue(POST_QUEUE_NAME, Buffer.from(message));
    }
});

jetstream.onDelete('app.bsky.feed.post', (event) => {
    const message = JSON.stringify({
        operation: event.commit.operation,
        uri: createUri(event.did, event.commit.collection, event.commit.rkey),
    });
    channel.sendToQueue(POST_QUEUE_NAME, Buffer.from(message));
});

jetstream.onCreate('app.bsky.feed.like', (event) => {
    const message = JSON.stringify({
        operation: event.commit.operation,
        did: event.did,
    });
    channel.sendToQueue(LIKE_QUEUE_NAME, Buffer.from(message));
});

jetstream.onDelete('app.bsky.feed.like', (event) => {
    const message = JSON.stringify({
        operation: event.commit.operation,
        did: event.did,
    });
    channel.sendToQueue(LIKE_QUEUE_NAME, Buffer.from(message));
})

jetstream.start();

/**
 * URIを作成します。
 * @param did DID
 * @param collection Collection 
 * @param rkey rkey
 * @returns URI
 */
function createUri(did: string, collection: string, rkey: string) {
    return `at://${did}/${collection}/${rkey}`;
}

/**
 * producerをシャットダウンします。
 */
async function shutdown() {
    try {
        console.log('producerをシャットダウンします');
        jetstream.close();
    } catch (error) {
        console.log(`シャットダウン中にエラーが発生しました: ${error}`);
        process.exit(1);
    } finally {
        await channel.close();
        await conn.close();
    }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
