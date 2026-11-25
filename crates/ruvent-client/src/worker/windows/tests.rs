use super::*;

fn book(slots: u32, servers: &[&'static str]) -> Book<&'static str> {
    let mut book = Book::new(slots);
    for server in servers {
        book.add(*server);
    }
    book
}

fn total(book: &Book<&'static str>) -> u32 {
    book.seats().values().map(|seat| seat.window).sum()
}

fn window(book: &Book<&'static str>, server: &'static str) -> u32 {
    book.seat(&server).unwrap().window
}

#[test]
fn slots_with_no_place_go_to_the_smallest_connected_window_and_never_exceed_p() {
    let mut book = book(4, &["a", "b"]);
    let changes = book.connect(&"a");
    assert_eq!(changes.last(), Some(&("a", 4)), "the only connected server takes every slot");
    book.connect(&"b");
    assert_eq!((window(&book, "a"), window(&book, "b")), (4, 0), "placed slots stay until a server starves");
    assert_eq!(total(&book), 4);
    let mut even = Book::new(5);
    for server in ["a", "b", "c"] {
        even.add(server);
    }
    for server in ["a", "b", "c"] {
        even.seats.get_mut(server).unwrap().connected = true;
    }
    even.connect(&"a");
    let windows: Vec<u32> = even.seats().values().map(|seat| seat.window).collect();
    assert!(windows.iter().max().unwrap() - windows.iter().min().unwrap() <= 1, "{windows:?}");
}

#[test]
fn a_starving_server_takes_one_slot_from_the_largest_idle_window() {
    let mut book = book(4, &["a", "b"]);
    book.connect(&"a");
    book.connect(&"b");
    // a holds 4 with nothing running; b starves.
    let changes = book.pending(&"b");
    assert_eq!(changes, [("a", 3), ("b", 1)]);
    assert_eq!(total(&book), 4);
    assert!(book.seat(&"b").unwrap().starving, "a granted slot is not an idle window: the server fills it");
    assert_eq!(book.assigned(&"b", false).0, Arrival::Run);
    assert!(!book.seat(&"b").unwrap().starving, "its filling assignment said nothing more waits");
}

#[test]
fn a_run_end_moves_a_slot_only_when_the_ender_is_two_above_the_receiver() {
    let mut book = book(8, &["a", "b"]);
    book.connect(&"a");
    book.connect(&"b");
    for _ in 0..7 {
        assert_eq!(book.assigned(&"a", false).0, Arrival::Run);
    }
    assert_eq!(book.assigned(&"a", true), (Arrival::Run, Vec::new()), "a starves, nobody has idle slots");
    assert!(book.pending(&"b").is_empty(), "no idle window anywhere: nothing moves on a signal");
    // Runs end on a: one slot per end until the two are within one.
    let mut windows = Vec::new();
    for _ in 0..8 {
        book.ended(&"a");
        windows.push((window(&book, "a"), window(&book, "b")));
        // Saturated: the server refills a freed slot at once with `more`
        // (before anything else is said); a full window hears `Pending`.
        for server in ["a", "b"] {
            if book.seat(&server).unwrap().idle() > 0 {
                book.assigned(&server, true);
            } else {
                book.pending(&server);
            }
        }
    }
    assert_eq!(windows[..4], [(7, 1), (6, 2), (5, 3), (4, 4)]);
    assert!(windows[4..].iter().all(|pair| *pair == (4, 4)), "saturated and even: no more moves, {windows:?}");
}

#[test]
fn saturation_evens_the_windows_to_within_one_and_holds_the_invariant() {
    let mut book = book(6, &["a", "b", "c", "d"]);
    book.connect(&"a");
    for server in ["b", "c", "d"] {
        book.connect(&server);
    }
    // Every server has work: whatever is idle is taken at once, with `more`.
    let mut ends = 0;
    for round in 0..200 {
        for server in ["a", "b", "c", "d"] {
            if book.seat(&server).unwrap().idle() > 0 {
                book.assigned(&server, true);
            } else {
                book.pending(&server);
            }
        }
        assert!(total(&book) <= 6, "the invariant holds");
        let busy: Vec<&str> = book.seats().iter().filter(|(_, seat)| seat.in_flight > 0).map(|(server, _)| *server).collect();
        if !busy.is_empty() {
            book.ended(&busy[round % busy.len()]);
            ends += 1;
        }
    }
    assert!(ends > 100);
    let windows: Vec<u32> = book.seats().values().map(|seat| seat.window).collect();
    assert!(windows.iter().max().unwrap() - windows.iter().min().unwrap() <= 1, "{windows:?}");
    assert!(windows.iter().all(|window| *window >= 1), "with P >= S every starving server holds one or more: {windows:?}");
}

#[test]
fn p_below_s_reaches_a_zero_window_server_as_soon_as_a_slot_goes_idle() {
    let mut book = book(1, &["a", "b", "c", "d"]);
    for server in ["a", "b", "c", "d"] {
        book.connect(&server);
    }
    let holder = *book.seats().iter().find(|(_, seat)| seat.window == 1).unwrap().0;
    let other = *["a", "b", "c", "d"].iter().find(|server| **server != holder).unwrap();
    book.assigned(&holder, false);
    assert!(book.pending(&other).is_empty(), "nothing idle anywhere yet");
    // The holder's run ends and its server has nothing more: the slot is idle.
    let changes = book.ended(&holder);
    assert_eq!(changes, [(holder, 0), (other, 1)], "the starving server gets the slot at the run's end");
    assert_eq!(total(&book), 1);
}

#[test]
fn a_disconnected_server_keeps_what_its_runs_hold_and_frees_the_rest() {
    let mut book = book(4, &["a", "b"]);
    book.connect(&"a");
    book.connect(&"b");
    book.assigned(&"a", false);
    let changes = book.disconnect(&"a");
    assert_eq!(window(&book, "a"), 1, "its run keeps its slot");
    assert_eq!(changes, [("b", 3)], "the freed slots went to the connected server, one value per server");
    book.ended(&"a");
    assert_eq!((window(&book, "a"), window(&book, "b")), (0, 4), "the ended run's slot found a place");
    let changes = book.connect(&"a");
    assert_eq!(changes.first(), Some(&("a", 0)), "a reconnected server is told its value");
}

#[test]
fn a_pending_with_a_known_idle_window_says_the_value_again() {
    let mut book = book(2, &["a"]);
    book.connect(&"a");
    assert_eq!(book.pending(&"a"), [("a", 2)]);
    assert!(!book.seat(&"a").unwrap().starving);
}

#[test]
fn an_assignment_past_the_window_overflows_and_a_closed_book_opens_nothing() {
    let mut book = book(1, &["a"]);
    book.connect(&"a");
    assert_eq!(book.assigned(&"a", false).0, Arrival::Run);
    assert_eq!(book.assigned(&"a", false).0, Arrival::Overflow);
    assert_eq!(book.close(), [("a", 0)]);
    book.ended(&"a");
    assert_eq!(window(&book, "a"), 0);
    assert_eq!(book.assigned(&"a", false).0, Arrival::Overflow);
}
