# ruvent-proxy

프로토콜별 프록시 바이너리(`ruvent-proxy-ws`, `ruvent-proxy-http`)가 함께 쓰는 라이브러리. 프록시에는 사용자 코드가 없고, 판단은 모두 JSON 설정에서 온다(제안서 4.3, 6.5, 6.8).

| 모듈 | 내용 |
| --- | --- |
| `config` | JSON 설정. 예시: [proxy.example.json](proxy.example.json) |
| `auth` | 최종 사용자 인증. `none`, `static`(설정의 토큰, Q1(c)), `worker`(인증 워커가 착수용 이벤트로 판정, Q1(a)) |
| `hub` | 상위 연결 하나, 채널마다 상위 구독 하나, 사용자 팬아웃 |
| `message` | 사용자 JSON 형식(두 바이너리 공용) |
| `upstream` | 상위 연결 공급자. `Remote`(이진 프로토콜), `InProcess`(같은 프로세스의 엔진) |

## 설정

| 키 | 뜻 | 기본값 |
| --- | --- | --- |
| `listen` | 프록시가 듣는 주소 | 필수 |
| `upstream.address`, `upstream.token` | 이벤트 서버 주소와 이 프록시의 서비스 자격 토큰 | 필수 |
| `auth.mode` | `none` / `static` / `worker` | `none` |
| `auth.tokens` | `static`용 `[{token, user, channels}]`. `channels`가 비면 기본 채널 | `[]` |
| `auth.worker` | `worker`용 `{channel, type, reply_channel, timeout_ms}` | — |
| `channels` | 인증이 따로 정하지 않을 때 사용자가 쓸 수 있는 채널. 정확한 이름 또는 `*`로 끝나는 접두사 | `["*"]` |
| `user_buffer` | 사용자 송신 버퍼 메시지 수. 가득 차면 그 사용자를 끊는다(손실 신호) | `1024` |
| `delegate` | 등록에 최종 사용자를 `delegate`로 싣는다. 상위 자격에 위임 권한이 있어야 한다 | `true` |
| `reconnect.initial_ms`, `reconnect.max_ms` | 상위 재접속 백오프의 첫 대기와 상한 | `100`, `5000` |

## 동작

* **구독 집약.** 이벤트 서버 쪽 구독은 채널마다 하나다. 사용자가 가입할 때마다 프록시는 상위에 구독을 한 번 더 보내고(서버는 같은 연결의 중복 구독을 하나로 둔다) 그 응답의 `Subscribed` 표지를 읽은 뒤에 그 사용자를 활성으로 만든다. 표지 이전에 받은 이벤트는 이미 활성이던 사용자에게만 간다. 그래서 늦게 붙은 사용자와 `0→1→0→1` 재구독에 옛 이벤트가 섞이지 않는다.
* **마지막 사용자가 떠나면** 상위 구독을 취소한다. 구독·취소는 연결마다 한 작업자가 낸 순서대로 보낸다.
* **상위가 끊기면** 백오프로 재접속하고, 사용자가 남아 있는 채널을 모두 다시 구독한다. 끊긴 동안의 발송은 잃는다(Q8(a)).
* **인증 워커(Q1(a)).** 프록시는 `auth.worker.channel`에 payload `{"token":…}`, 무작위 correlation, `reply`=`auth.worker.reply_channel`인 착수용 이벤트를 등록한다. 인증 워커는 그 이벤트를 완료하면서 같은 correlation으로 reply 채널에 발송용 후속 `{"ok":true|false,"user":"…","channels":["…"]}`를 낸다. `timeout_ms` 안에 답이 없으면 거절한다.

## 타이머

이 크레이트가 시계를 기다리는 곳은 둘이다: 상위 재접속 백오프(`hub`), 인증 워커 답의 기한(`hub`, `Proxy::authenticate`).

## 사용자 JSON

`src/message/mod.rs` 머리 주석이 원본이다.

```json
{"op":"auth","token":"…"}
{"op":"publish","kind":"claimable|dispatch","channel":"…","type":"…","payload":{},"id":"<uuid>","reply":"…","correlation":"<uuid>","ref":1}
{"op":"subscribe","channel":"…","ref":1}
{"op":"unsubscribe","channel":"…","ref":1}
→ {"op":"event","channel":"…","type":"…","id":"<uuid>","payload":{},"correlation":"<uuid>"}
→ {"op":"result","ref":1,"ok":true,"id":"<uuid>","error":"…"}
```

## 테스트

`cargo test -p ruvent-proxy` — `tests/hub.rs`가 같은 프로세스의 엔진을 상위로 두고 집약, 늦은 가입, 재구독, 상위 구독 취소, 느린 사용자 차단, 채널 범위, 위임 신원, 정적·워커 인증, 상위 재접속을 판정한다.
