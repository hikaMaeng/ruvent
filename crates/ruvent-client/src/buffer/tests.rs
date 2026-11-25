use super::*;

fn filled(count: u64) -> Unacked<u64> {
    let mut buffer = Unacked::new();
    for item in 0..count {
        assert_eq!(buffer.push(item, 10), item + 1);
    }
    buffer
}

#[test]
fn a_cumulative_acknowledgement_empties_the_buffer_up_to_it() {
    let mut buffer = filled(5);
    assert_eq!(buffer.bytes(), 50);
    let done = buffer.ack(3);
    assert_eq!(done.iter().map(|entry| entry.seq).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!((buffer.len(), buffer.bytes()), (2, 20));
    assert!(buffer.ack(2).is_empty(), "an older acknowledgement drops nothing");
    buffer.ack(5);
    assert!(buffer.is_empty());
    assert_eq!(buffer.bytes(), 0);
    assert_eq!(buffer.push(9, 1), 6, "numbers go on");
}

#[test]
fn resume_drops_what_was_applied_and_keeps_the_numbers_of_the_rest() {
    let mut buffer = filled(5);
    let (dropped, resumed) = buffer.resume(2);
    assert_eq!(dropped.len(), 2);
    assert_eq!(resumed, Resumed { dropped: 2, renumbered: false });
    assert_eq!(buffer.iter().map(|entry| entry.seq).collect::<Vec<_>>(), [3, 4, 5]);
    assert_eq!(buffer.next(), 6);
}

#[test]
fn a_server_that_lost_the_record_gets_the_rest_numbered_from_resume_plus_one() {
    let mut buffer = filled(5);
    buffer.ack(3);
    let (_, resumed) = buffer.resume(0);
    assert!(resumed.renumbered);
    assert_eq!(buffer.iter().map(|entry| (entry.seq, entry.item)).collect::<Vec<_>>(), [(1, 3), (2, 4)]);
    assert_eq!(buffer.next(), 3);
    let mut empty = filled(4);
    empty.ack(4);
    let (_, resumed) = empty.resume(0);
    assert!(!resumed.renumbered);
    assert_eq!(empty.push(0, 1), 1, "an empty buffer continues right after resume");
}
