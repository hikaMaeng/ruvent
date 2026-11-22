# ruvent-proxy-http

HTTP 프록시 바이너리. 등록은 `POST`, 구독은 SSE(server-sent events).

```sh
ruvent-proxy-http http.example.json
```

| 경로 | 동작 |
| --- | --- |
| `POST /v1/events` | 본문은 publish(`op` 없는 사용자 JSON), `Authorization: Bearer <token>`. 200 등록, 401 인증 실패, 403 채널 범위 밖, 409 같은 id, 503 상위 없음, 400 그 밖 |
| `GET /v1/stream?channel=a&channel=b` | 모든 구독이 반영된 뒤 응답을 시작하고, 이벤트 JSON을 `data:` 한 줄씩 보낸다. 사용자 버퍼가 차면 스트림을 끝낸다 |
| `GET /health` | 상위 연결이 살아 있으면 200, 아니면 503 |

요청마다 따로 인증한다. `worker` 모드에서는 요청마다 이벤트 왕복이 한 번 든다. SSE 유지용 주기 핑은 보내지 않는다(타이머 없음).

설정 예시: [http.example.json](http.example.json). 키는 [ruvent-proxy README](../ruvent-proxy/README.md#설정).

테스트: `cargo test -p ruvent-proxy-http` — `tests/smoke.rs`가 실제 TCP로 POST가 SSE 구독자에게 닿는지, 잘못된 토큰(401)과 범위 밖 채널(403)을 확인한다.
