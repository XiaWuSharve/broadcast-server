const WebSocket = require("ws");
const WebSocketServer = WebSocket.Server;
const wss = new WebSocketServer({ port: 3001, });

const Status = {
    SUCCESS: 0,
    FAILED: 1,
}

//  存储连接的客户端
const people = {};
wss.on("connection", function (ws) {
    ws.on("message", function (message) {
        message = message.toString();
        message = JSON.parse(message);

        switch (message.type) {
            case "connect":
                // 将连接的客户端存储起来
                const id = message.data;
                console.log('connect: ', id);
                people[id] = {
                    id: id,
                    ws,
                };
                ws.send(
                    JSON.stringify({
                        type: "connect",
                        data: Status.SUCCESS,
                    })
                );
                break;
            case "call":
                // 将 sdp 发给接收端，sessionId 为 接收端的 id
                const sdp = message.data.sdp;
                const sId = message.data.remoteId;
                const fromId = message.data.localId;
                console.log('call: received: ', message.data);
                if (people[sId]) {
                    people[sId].ws.send(
                        JSON.stringify({
                            type: "call",
                            data: {
                                sdp,
                                remoteId: fromId
                            },
                        })
                    );
                    console.log('call: ', sdp);
                }
                break;
            case "answer":
                // 接收端将 sdp 发给发起端，sessionId 为 发起端的 id
                const answerSDP = message.data.sdp;
                const recevId = message.data.sessionId;
                console.log('answer: received: ', message.data)
                if (people[recevId]) {
                    people[recevId].ws.send(
                        JSON.stringify({
                            type: "answer",
                            data: answerSDP,
                        })
                    );
                    console.log('answer: ', answerSDP);
                }

                break;
            case "candidate":
                // 接收端将 sdp 发给发起端，sessionId 为 发起端的 id
                const candidate = message.data;
                const cId = message.data.sessionId;
                console.log('candidate: ', message.data);
                if (people[cId]) {
                    people[cId].ws.send(
                        JSON.stringify({
                            type: "candidate",
                            data: candidate
                        })
                    );
                    console.log('candidate: ', candidate);
                }

                break;
            case "getAllClients":
                ws.send(
                    JSON.stringify({
                        type: "getAllClients",
                        data: Object.keys(people),
                    })
                );
                // console.log('getAllClients: ', Object.keys(people));
                break;
        }
    });
});

