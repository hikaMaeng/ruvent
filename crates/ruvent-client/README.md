# ruvent-client

이벤트 서버에 붙는 쪽이다. 두 부분으로 나뉜다.

* `remote` — 소켓 연결. `remote::connect(주소, 토큰, Options)`가 인프로세스 연결과 같은 `ruvent::Client`와
  `Inbox`를 돌려준다. 메서드 구현은 코어의 `Client` 하나뿐이고, 여기는 링크(쓰기·읽기 태스크)만 붙인다.
* `worker` — 워커 프레임워크. `ruvent::Methods`만 보므로 같은 서비스를 내장 엔진에도, 원격 서버에도 붙인다.

## 워커 프레임워크

```rust
use ruvent_client::worker::{App, Service};
use std::time::Duration;

# async fn demo(client: ruvent::Client, inbox: ruvent::Inbox) {
let running = App::new(client, inbox)
    .service(
        Service::new("orders")
            .on("create", |cx| async move {
                cx.emit_dispatch("orders.events", "created", cx.payload().clone());
                Ok(())
            })
            .permits(16)
            .lease(Duration::from_secs(30))
            .max_retries(5)
            .failures("orders.failed"),
    )
    .service(Service::executor("jobs").on("render", |_cx| async move { Ok(()) }))
    .start()
    .await
    .unwrap();
running.wait().await.unwrap_err(); // 연결이 끝나면 오류로 돌아온다
# }
```

| 설정 | 기본값 | 뜻 |
| --- | --- | --- |
| `permits` | 16 (실행기 4) | 서비스별 독립 허가 예산. 허가는 핸들러가 끝나는 즉시 돌아온다(완료 응답을 기다리지 않는다) |
| `lease` | 30초 (실행기 1시간) | 착수 기한. 핸들러가 넘기면 완료 보고 없이 버리고, 서버가 다음 등록 때 회수한다 |
| `max_retries` | 5 | 엔벨롭 `retry`가 이를 넘으면 핸들러를 돌리지 않고 완료한다 |
| `failures` | 없음 | 설정하면 실패·재시도 초과 때 완료의 후속으로 실패 발송 이벤트를 낸다(payload = 사유, correlation = 실패한 id) |
| `pending_limit` | 1024 | 허가가 없을 때 보관하는 알림 상한. 넘쳐 버리면 다음 여유 때 대기분을 처음부터 다시 읽는다 |

* 흐름: 시작 때 채널 구독과 대기분 이어받기 → 알림마다 허가가 있으면 착수 → 핸들러 → 모은 후속과 함께 완료.
  착수 응답의 작업 키가 알림의 키와 다르면 새 일로 다룬다.
* 실패(핸들러 `Err`, panic)는 이벤트를 완료 처리한다. 다음 처리가 필요하면 핸들러가 새 이벤트를 낸다.
* `App::listen(채널, 함수)`는 발송용 이벤트를 받는다.
* `Running::wait()`는 연결이 끝나면 `Err(Disconnected)`로, `shutdown()` 뒤에는 `Ok`로 끝난다.
