use super::*;

#[test]
fn ranges_merge_and_membership_follows_them() {
    let set = TypeSet::from_ranges([(10, 19), (1, 3), (4, 5), (30, 30), (2, 2), (9, 1)]);
    assert_eq!(set.ranges(), &[(1, 5), (10, 19), (30, 30)], "adjacent and overlapping ranges merge; an empty one is dropped");
    for kind in [1, 5, 10, 19, 30] {
        assert!(set.contains(kind), "{kind}");
    }
    for kind in [0, 6, 9, 20, 29, 31, u16::MAX] {
        assert!(!set.contains(kind), "{kind}");
    }
    assert!(set.contains_all(&[1, 12, 30]));
    assert!(!set.contains_all(&[1, 6]));
}

#[test]
fn every_and_no_type() {
    assert!(TypeSet::all().contains(0) && TypeSet::all().contains(u16::MAX) && TypeSet::all().is_all());
    assert!(TypeSet::none().is_empty() && !TypeSet::none().contains(0));
    assert!(TypeSet::from_ranges([(0, 100), (101, u16::MAX)]).is_all());
}

#[test]
fn the_text_form() {
    assert_eq!(TypeSet::parse(&["7", " 10 - 12 ", "8"]), Ok(TypeSet::from_ranges([(7, 8), (10, 12)])));
    assert_eq!(TypeSet::parse(&["*"]), Ok(TypeSet::all()));
    assert_eq!(TypeSet::parse::<&str>(&[]), Ok(TypeSet::none()));
    for bad in ["x", "70000", "5-3", "-1", "1-", ""] {
        assert!(TypeSet::parse(&[bad]).is_err(), "{bad:?}");
    }
}
