# ruvent-protocol

이벤트 서버와 접속자 사이의 이진 와이어 형식, wire v2(계획 2.2, 2.11)다. 서버([`ruvent-server`](../ruvent-server/README.md))와
원격 연결([`ruvent-client`](../ruvent-client/README.md)의 `remote`)이 함께 쓴다. 이벤트의 머리는
`ruvent::envelope::codec`의 레이아웃에서 본문 블록을 뺀 것이다(`encode_head`).

## 형식

* 레코드 = `[tag u8][head_len u16][body_len u32][head][body]`(리틀 엔디언). 바깥 프레임은 없다.
* 본문은 이벤트의 payload이고 레코드마다 많아야 하나다. 후속이 여럿인 완료는 `Complete` 뒤에 `FollowUp`을,
  구독 답은 `Subscribed` 뒤에 `Notice`를 그 수만큼 잇는다. 디코더가 모아 메시지 하나로 넘긴다.
* 머리 안의 수(순번, 키, 길이, 개수, 시각)는 부호 없는 LEB128 가변 길이 정수다. id·nonce·group은 16바이트,
  참거짓은 1바이트(0/1), 문자열은 `[가변 길이][UTF-8]`.
* 이름(채널·타입·주체·위임 신원)은 방향마다 연결별 사전 번호다(`NameDef`, `DictReset`, 상한 S18 4096).
* 첫 레코드는 `Hello`다(버전 2, 기능 비트, 역할, 토큰, producer id, 인스턴스 nonce, 보존 기간, 확인 끔, 쓸 채널).
  서버는 `Welcome`(버전, 기능 비트, server id, 세대, 자격, `resume`, 밀어냄) 또는 `Bye(사유)`로 답한다.
* 태그 값 표와 기능 비트 표는 `src/lib.rs`의 모듈 문서에 있다.

다음은 레코드를 거절하고 연결을 닫는다: 모르는 태그, 반대 방향이나 제자리가 아닌 레코드, 금지된 필드 조합,
머리에 남는 바이트, 정의되지 않은 이름, 같은 번호의 재정의, 사전 상한 초과, 본문 상한(B13, 16 MiB) 초과,
본문이 없는 레코드의 본문. v1은 거절만 한다(B12).

## API

| 항목 | 내용 |
| --- | --- |
| `Encoder` | 메시지 → 레코드. 메시지를 통째로 먼저 검사하므로 거절돼도 사전 정의를 남기지 않는다. `set_now`(배정의 남은 lease), `with_inline`(S11), `with_max_body`, `with_features`, `take() -> Parts` |
| `Decoder` | 레코드 → 메시지. `admit`(본문 읽기 전에 머리 검사) → 본문 → `decode`. `follow_up`(후속의 채널을 본문 전에) |
| `RecordReader` | 머리 먼저 읽기. `body`(작은 본문은 복사, `direct` 이상은 소켓에서 직접), `chunk`(디스크로 흘려 읽기), `discard`(단계로 닫기), `peek` |
| `Parts` | 벡터 쓰기(`write_to`). 부분 쓰기를 남은 곳에서 잇고 쓴 바이트(`written`)를 센다 |
| `validate` | 인코더가 받을 메시지인지. 순번을 매기기 전에 거른다 |
| `Message`, `Body`, `Side`, `ProtocolError`, `features`, `sniff_v1`, `io::v1_bye` | |
