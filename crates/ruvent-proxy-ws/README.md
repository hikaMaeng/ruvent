# ruvent-proxy-ws

WebSocket 프록시 바이너리. 사용자 하나에 WebSocket 하나, 텍스트 프레임에 [ruvent-proxy](../ruvent-proxy/README.md)의 사용자 JSON을 싣는다.

```sh
ruvent-proxy-ws ws.example.json
```

| 경로 | 동작 |
| --- | --- |
| `GET /v1/ws?token=…` | 업그레이드. `token`이 없고 `auth.mode`가 `none`이 아니면 첫 메시지가 `{"op":"auth","token":"…"}`여야 한다. 인증되면 `{"op":"result","ok":true}` |
| `GET /health` | 상위 연결이 살아 있으면 200, 아니면 503 |

설정 예시: [ws.example.json](ws.example.json). 키는 [ruvent-proxy README](../ruvent-proxy/README.md#설정).

사용자 송신 버퍼가 가득 차면 프록시가 소켓을 닫는다. 닫힘이 손실 신호다.

테스트: `cargo test -p ruvent-proxy-ws` — `tests/smoke.rs`가 실제 TCP로 라우터를 띄워 인증·구독·수신·발행, 잘못된 토큰 거절, `/health`를 확인한다.
