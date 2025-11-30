const axios = require("axios");

class Consumer {
    constructor(brokerUrl, topic, startOffset = 0) {
        this.brokerUrl = brokerUrl;
        this.topic = topic;
        this.offset = startOffset;
    }

    async poll() {
        const res = await axios.get(`${this.brokerUrl}/fetch`, {
            params: {
                topic: this.topic,
                offset: this.offset,
                max_bytes: 4096
            }
        });

        const records = res.data.records;

        for (const r of records) {
            console.log(`Received (offset=${r.offset}): ${r.payload}`);
            this.offset = r.offset + 1;   // move offset forward
        }

        return records.length > 0;
    }
}

// demo polling loop
(async () => {
    const consumer = new Consumer("http://localhost:5000", "demo-topic");

    setInterval(async () => {
        const gotMessages = await consumer.poll();
        if (!gotMessages) {
            console.log("No new messages...");
        }
    }, 1000);
})();

