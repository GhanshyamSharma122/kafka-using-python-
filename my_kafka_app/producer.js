const axios = require("axios");

class Producer {
    constructor(brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    async send(topic, payload) {
        const res = await axios.post(`${this.brokerUrl}/produce`, {
            topic,
            payload
        });
        return res.data.offset;
    }
}

// demo
(async () => {
    const producer = new Producer("http://localhost:5000");

    for (let i = 0; i < 5; i++) {
        const offset = await producer.send("demo-topic", `hello ${i}`);
        console.log("Sent message, offset =", offset);
    }
})();

